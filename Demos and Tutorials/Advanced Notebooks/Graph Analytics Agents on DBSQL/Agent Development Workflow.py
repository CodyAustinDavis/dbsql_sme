# Databricks notebook source
# MAGIC %md
# MAGIC #Tool-calling Agent
# MAGIC
# MAGIC This is an auto-generated notebook created by an AI Playground export.
# MAGIC
# MAGIC This notebook uses [Mosaic AI Agent Framework](https://docs.databricks.com/generative-ai/agent-framework/build-genai-apps.html) to recreate your agent from the AI Playground. It  demonstrates how to develop, manually test, evaluate, log, and deploy a tool-calling agent in LangGraph.
# MAGIC
# MAGIC The agent code implements [MLflow's ChatAgent](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ChatAgent) interface, a Databricks-recommended open-source standard that simplifies authoring multi-turn conversational agents, and is fully compatible with Mosaic AI agent framework functionality.
# MAGIC
# MAGIC  **_NOTE:_**  This notebook uses LangChain, but AI Agent Framework is compatible with any agent authoring framework, including LlamaIndex or pure Python agents written with the OpenAI SDK.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow-skinny[databricks] langgraph==0.3.4 databricks-langchain databricks-agents langchain-community langchain databricks-sql-connector uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Define the agent in code
# MAGIC Below we define our agent code in a single cell, enabling us to easily write it to a local Python file for subsequent logging and deployment using the `%%writefile` magic command.
# MAGIC
# MAGIC For more examples of tools to add to your agent, see [docs](https://docs.databricks.com/generative-ai/agent-framework/agent-tool.html).

# COMMAND ----------

# MAGIC %%writefile agent.py
# MAGIC from typing import Any, Generator, Optional, Sequence, Union,Iterable, List, Dict, Any
# MAGIC import mlflow
# MAGIC from databricks_langchain import (
# MAGIC     ChatDatabricks,
# MAGIC     VectorSearchRetrieverTool,
# MAGIC     DatabricksFunctionClient,
# MAGIC     UCFunctionToolkit,
# MAGIC     set_uc_function_client,
# MAGIC )
# MAGIC from unitycatalog.ai.langchain.toolkit import UnityCatalogTool
# MAGIC from langchain_core.language_models import LanguageModelLike
# MAGIC from langchain_core.runnables import RunnableConfig, RunnableLambda
# MAGIC from langchain_core.tools import BaseTool, Tool, StructuredTool
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from langgraph.graph.graph import CompiledGraph
# MAGIC from langgraph.graph.state import CompiledStateGraph
# MAGIC from langgraph.prebuilt.tool_node import ToolNode
# MAGIC from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
# MAGIC from mlflow.pyfunc import ChatAgent
# MAGIC from mlflow.types.agent import (
# MAGIC     ChatAgentChunk,
# MAGIC     ChatAgentMessage,
# MAGIC     ChatAgentResponse,
# MAGIC     ChatContext,
# MAGIC )
# MAGIC import os
# MAGIC from langchain_community.utilities.sql_database import SQLDatabase
# MAGIC from pydantic import BaseModel, Field
# MAGIC from databricks import sql
# MAGIC
# MAGIC mlflow.langchain.autolog()
# MAGIC
# MAGIC client = DatabricksFunctionClient()
# MAGIC set_uc_function_client(client)
# MAGIC
# MAGIC ############################################
# MAGIC # Define your LLM endpoint and system prompt
# MAGIC ############################################
# MAGIC LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"
# MAGIC WAREHOUSE_ID = "<WAREHOUSE_ID>"
# MAGIC
# MAGIC llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
# MAGIC
# MAGIC system_prompt = """This is a Graph Analytics Agent for cyber threat detection data with STIX dataset. Your goal is to analyze that graph and product threat reports. 
# MAGIC """
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Define tools for your agent, enabling it to retrieve data or take actions
# MAGIC ## beyond text generation
# MAGIC ## To create and see usage examples of more tools, see
# MAGIC ## https://docs.databricks.com/generative-ai/agent-framework/agent-tool.html
# MAGIC ###############################################################################
# MAGIC tools = []
# MAGIC
# MAGIC # You can use UDFs in Unity Catalog as agent tools
# MAGIC uc_tool_names = ["main.threat_graph_analytics.get_node_types","main.threat_graph_analytics.get_relationship_types", "main.threat_graph_analytics.get_node_metadata","main.threat_graph_analytics.get_top_connected_nodes"]
# MAGIC
# MAGIC uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
# MAGIC
# MAGIC tools.extend(uc_toolkit.tools)
# MAGIC
# MAGIC ### Adding Custom tools in Langchain directly
# MAGIC
# MAGIC # pip install --upgrade langchain langchain-community databricks-sqlalchemy databricks-sql-connector databricks-langchain
# MAGIC import os
# MAGIC from langchain_community.utilities.sql_database import SQLDatabase
# MAGIC from langchain_core.tools import Tool
# MAGIC from pydantic import BaseModel, Field
# MAGIC
# MAGIC from databricks import sql
# MAGIC from typing import Iterable, List, Dict, Any, Optional
# MAGIC
# MAGIC class DatabricksQueryError(RuntimeError): ...
# MAGIC class NonSelectQueryError(ValueError): ...
# MAGIC
# MAGIC def _validate_select_only(query: str) -> None:
# MAGIC     q = query.strip().lower()
# MAGIC     if not (q.startswith("select") or q.startswith("with") or q.startswith("call")):
# MAGIC         raise NonSelectQueryError("Only SELECT statements are allowed.")
# MAGIC
# MAGIC def run_query(
# MAGIC     query: str,
# MAGIC     params: Optional[tuple] = None,
# MAGIC     as_dict: bool = True,
# MAGIC     array_size: int = 0,  # 0 = fetchall
# MAGIC ) -> List[Dict[str, Any]] | list:
# MAGIC     """Run a read-only SQL query on Databricks.
# MAGIC     - params: DB-API parameters (use '?' placeholders in SQL)
# MAGIC     - as_dict: if True, return list of dicts; else list of tuples
# MAGIC     - array_size: if >0, fetchmany(array_size) in a loop
# MAGIC     """
# MAGIC     _validate_select_only(query)
# MAGIC
# MAGIC     try:
# MAGIC         with sql.connect(
# MAGIC             server_hostname="e2-demo-field-eng.cloud.databricks.com",
# MAGIC             http_path= f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
# MAGIC             access_token="<TOKEN_OR_SECRET>",
# MAGIC         ) as conn, conn.cursor() as cur:
# MAGIC             cur.execute(query, params or ())
# MAGIC             cols = [d[0] for d in (cur.description or [])]
# MAGIC
# MAGIC             if array_size and array_size > 0:
# MAGIC                 rows_all = []
# MAGIC                 while True:
# MAGIC                     chunk = cur.fetchmany(array_size)
# MAGIC                     if not chunk:
# MAGIC                         break
# MAGIC                     if as_dict:
# MAGIC                         rows_all.extend([dict(zip(cols, r)) for r in chunk])
# MAGIC                     else:
# MAGIC                         rows_all.extend(chunk)
# MAGIC                 return rows_all
# MAGIC
# MAGIC             rows = cur.fetchall()
# MAGIC             if as_dict:
# MAGIC                 return [dict(zip(cols, r)) for r in rows]
# MAGIC             return rows
# MAGIC     except Exception as e:
# MAGIC         raise DatabricksQueryError(f"Databricks query failed: {e}") from e
# MAGIC
# MAGIC
# MAGIC # Define the schema for the tool input
# MAGIC class QueryInput(BaseModel):
# MAGIC     query: str = Field(..., description="A SELECT-only SQL query to run on Databricks.")
# MAGIC
# MAGIC # Your execution function
# MAGIC def run_databricks_query(query: str, params: Optional[tuple] = None) -> str:
# MAGIC     return run_query(query, params)
# MAGIC
# MAGIC # Create the tool object manually
# MAGIC databricks_sql_tool = Tool(
# MAGIC     name="databricks_sql_query",
# MAGIC     description="Run a read-only SQL query against the configured Databricks catalog.schema.",
# MAGIC     func=run_databricks_query,
# MAGIC     args_schema=QueryInput
# MAGIC )
# MAGIC
# MAGIC
# MAGIC # Append it to your existing list of tools
# MAGIC tools.append(databricks_sql_tool)
# MAGIC
# MAGIC #### Breadth first search stored procedure
# MAGIC class NeighborhoodInput(BaseModel):
# MAGIC     node_id: str = Field(..., description="Start node ID (e.g., a STIX ID).")
# MAGIC     max_level: int = Field(2, ge=1, le=10, description="Max hop distance from the start node.")
# MAGIC
# MAGIC
# MAGIC def run_recursive_neighborhood_query(node_id: str, max_level: int = 2) -> list[dict]:
# MAGIC     query = """CALL main.threat_graph_analytics.bfs_search(?,?)"""
# MAGIC     # Uses your existing DB-API runner with '?' binding
# MAGIC     return run_query(query, params=(node_id, int(max_level)), as_dict=True)
# MAGIC
# MAGIC
# MAGIC recursive_neighborhood_tool = StructuredTool.from_function(
# MAGIC     func=run_recursive_neighborhood_query,
# MAGIC     name="recursive_neighborhood_query",
# MAGIC     description=(
# MAGIC         "Breadth-first neighborhood from a starting node up to `max_level` hops. "
# MAGIC         "Returns rows with level, node_id, node_type, prev_node_id, relationship_type, and path."
# MAGIC     ),
# MAGIC     args_schema=NeighborhoodInput
# MAGIC )
# MAGIC
# MAGIC tools.append(recursive_neighborhood_tool)
# MAGIC
# MAGIC
# MAGIC #### Page Rank Tool
# MAGIC
# MAGIC #### Page Rank Tool
# MAGIC class PageRankModelInput(BaseModel):
# MAGIC     top_n: int = Field(10, ge=1, le=10, description="Top N nodes in page rank popularity.")
# MAGIC     node_types: Optional[List[str]] = Field(
# MAGIC         default=None,
# MAGIC         description="Optional list of node types to filter on (e.g., ['threat-actor', 'malware'])."
# MAGIC     )
# MAGIC
# MAGIC def run_page_rank_query(top_n: int = 10, node_types: Optional[List[str]] = None) -> list[dict]:
# MAGIC     if node_types:
# MAGIC         # Convert list to comma-separated string for SQL
# MAGIC         node_types_str = ",".join(node_types)
# MAGIC         query = """CALL main.threat_graph_analytics.pagerank_top_n(?, split(?, ','))"""
# MAGIC         params = (top_n, node_types_str)
# MAGIC     else:
# MAGIC         query = """CALL main.threat_graph_analytics.pagerank_top_n(?)"""
# MAGIC         params = (top_n,)
# MAGIC     return run_query(query, params=params, as_dict=True)
# MAGIC
# MAGIC page_rank_tool = StructuredTool.from_function(
# MAGIC     func=run_page_rank_query,
# MAGIC     name="page_rank_query",
# MAGIC     description=(
# MAGIC         "Page rank algorithm to get the top N most popular nodes, optionally filtered by a list of node types, by page rank influence."
# MAGIC     ),
# MAGIC     args_schema=PageRankModelInput
# MAGIC )
# MAGIC
# MAGIC tools.append(page_rank_tool)
# MAGIC
# MAGIC
# MAGIC #####################
# MAGIC ## Define agent logic
# MAGIC #####################
# MAGIC
# MAGIC def create_tool_calling_agent(
# MAGIC     model: LanguageModelLike,
# MAGIC     tools: Union[Sequence[BaseTool], ToolNode],
# MAGIC     system_prompt: Optional[str] = None,
# MAGIC ) -> CompiledGraph:
# MAGIC     
# MAGIC     model = model.bind_tools(tools)
# MAGIC
# MAGIC     # Define the function that determines which node to go to
# MAGIC     def should_continue(state: ChatAgentState):
# MAGIC         messages = state["messages"]
# MAGIC         last_message = messages[-1]
# MAGIC         # If there are function calls, continue. else, end
# MAGIC         if last_message.get("tool_calls"):
# MAGIC             return "continue"
# MAGIC         else:
# MAGIC             return "end"
# MAGIC
# MAGIC     if system_prompt:
# MAGIC         preprocessor = RunnableLambda(
# MAGIC             lambda state: [{"role": "system", "content": system_prompt}]
# MAGIC             + state["messages"]
# MAGIC         )
# MAGIC     else:
# MAGIC         preprocessor = RunnableLambda(lambda state: state["messages"])
# MAGIC     model_runnable = preprocessor | model
# MAGIC
# MAGIC     def call_model(
# MAGIC         state: ChatAgentState,
# MAGIC         config: RunnableConfig,
# MAGIC     ):
# MAGIC         response = model_runnable.invoke(state, config)
# MAGIC
# MAGIC         return {"messages": [response]}
# MAGIC
# MAGIC     workflow = StateGraph(ChatAgentState)
# MAGIC
# MAGIC     workflow.add_node("agent", RunnableLambda(call_model))
# MAGIC     workflow.add_node("tools", ChatAgentToolNode(tools))
# MAGIC
# MAGIC     workflow.set_entry_point("agent")
# MAGIC     workflow.add_conditional_edges(
# MAGIC         "agent",
# MAGIC         should_continue,
# MAGIC         {
# MAGIC             "continue": "tools",
# MAGIC             "end": END,
# MAGIC         },
# MAGIC     )
# MAGIC     workflow.add_edge("tools", "agent")
# MAGIC
# MAGIC     return workflow.compile()
# MAGIC
# MAGIC
# MAGIC class LangGraphChatAgent(ChatAgent):
# MAGIC     def __init__(self, agent: CompiledStateGraph):
# MAGIC         self.agent = agent
# MAGIC
# MAGIC     def predict(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> ChatAgentResponse:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC
# MAGIC         messages = []
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 messages.extend(
# MAGIC                     ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
# MAGIC                 )
# MAGIC         return ChatAgentResponse(messages=messages)
# MAGIC
# MAGIC     def predict_stream(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> Generator[ChatAgentChunk, None, None]:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 yield from (
# MAGIC                     ChatAgentChunk(**{"delta": msg}) for msg in node_data["messages"]
# MAGIC                 )
# MAGIC
# MAGIC
# MAGIC # Create the agent object, and specify it as the agent object to use when
# MAGIC # loading the agent back for inference via mlflow.models.set_model()
# MAGIC agent = create_tool_calling_agent(llm, tools, system_prompt)
# MAGIC AGENT = LangGraphChatAgent(agent)
# MAGIC
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since this notebook called `mlflow.langchain.autolog()` you can view the trace for each step the agent takes.
# MAGIC
# MAGIC Replace this placeholder input with an appropriate domain-specific example for your agent.

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

from agent import AGENT

AGENT.predict({"messages": [{"role": "user", "content": "What are my most influential threat actors and malware? Summarize the results into a clean description and bullet list."}]})

# COMMAND ----------

for event in AGENT.predict_stream(
    {"messages": [{"role": "user", "content": "What are my top threat actors?"}]}
):
    print(event, "-----------\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Determine Databricks resources to specify for automatic auth passthrough at deployment time
# MAGIC - **TODO**: If your Unity Catalog Function queries a [vector search index](https://docs.databricks.com/generative-ai/agent-framework/unstructured-retrieval-tools.html) or leverages [external functions](https://docs.databricks.com/generative-ai/agent-framework/external-connection-tools.html), you need to include the dependent vector search index and UC connection objects, respectively, as resources. See [docs](https://docs.databricks.com/generative-ai/agent-framework/log-agent.html#specify-resources-for-automatic-authentication-passthrough) for more details.
# MAGIC
# MAGIC Log the agent as code from the `agent.py` file. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import mlflow
from agent import LLM_ENDPOINT_NAME, tools
from databricks_langchain import VectorSearchRetrieverTool
from mlflow.models.resources import (
  DatabricksVectorSearchIndex,
  DatabricksServingEndpoint,
  DatabricksSQLWarehouse,
  DatabricksFunction,
  DatabricksGenieSpace,
  DatabricksTable,
  DatabricksUCConnection,
  DatabricksApp
)
from pkg_resources import get_distribution
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool

WAREHOUSE_ID = "8baced1ff014912d"

resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME),
             DatabricksSQLWarehouse(warehouse_id=WAREHOUSE_ID),
             DatabricksTable(table_name="main.threat_graph_analytics.full_synthetic_stix_graph"),
             DatabricksTable(table_name="main.threat_graph_analytics.stix_graph_nodes"),
             DatabricksTable(table_name="main.threat_graph_analytics.stix_graph_edges")
             ]

for tool in tools:
    if isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)

    elif isinstance(tool, UnityCatalogTool):
        # TODO: If the UC function includes dependencies like external connection or vector search, please include them manually.
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))

input_example = {
    "messages": [
        {
            "role": "user",
            "content": "Hi!"
        }
    ]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name="Threat Graph Analysis Agent",
        python_model="agent.py",
        input_example=input_example,
        resources=resources,
        pip_requirements=[
            f"databricks-connect=={get_distribution('databricks-connect').version}",
            f"mlflow=={get_distribution('mlflow').version}",
            f"databricks-langchain=={get_distribution('databricks-langchain').version}",
            f"langgraph=={get_distribution('langgraph').version}",
            f"databricks-sql-connector=={get_distribution('databricks-sql-connector').version}"
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://docs.databricks.com/mlflow3/genai/eval-monitor)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.
# MAGIC
# MAGIC Evaluate your agent with one of our [predefined LLM scorers](https://docs.databricks.com/mlflow3/genai/eval-monitor/predefined-judge-scorers), or try adding [custom metrics](https://docs.databricks.com/mlflow3/genai/eval-monitor/custom-scorers).

# COMMAND ----------

import mlflow
from mlflow.genai.scorers import RelevanceToQuery, Safety, RetrievalRelevance, RetrievalGroundedness


### This can be loaded from a Delta/Iceberg tabel as a set of unit tests
eval_dataset = [
    {
        "inputs": {
            "messages": [
                {
                    "role": "system",
                    "content": "This is a Graph Analytics Agent for cyber threat detection data with STIX dataset. Your goal is to analyze that graph and product threat reports. "
                },
                {
                    "role": "user",
                    "content": "What are my most influential threat actors and malware? Summarize the results into a clean description and bullet list."
                }
            ]
        },
        "expected_response": "Here are the top nodes with your requested input node types: Summary of mode influential nodes..."
    }
]

eval_results = mlflow.genai.evaluate(
    data=eval_dataset,
    predict_fn=lambda messages: AGENT.predict({"messages": messages}),
    scorers=[RelevanceToQuery(), Safety()], # add more scorers here if they're applicable
)

# Review the evaluation results in the MLfLow UI (see console output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform pre-deployment validation of the agent
# MAGIC Before registering and deploying the agent, we perform pre-deployment checks via the [mlflow.models.predict()](https://mlflow.org/docs/latest/python_api/mlflow.models.html#mlflow.models.predict) API. See [documentation](https://docs.databricks.com/machine-learning/model-serving/model-serving-debug.html#validate-inputs) for details

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/Threat Graph Analysis Agent",
    input_data={"messages": [{"role": "user", "content": "Hello!"}]},
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

catalog = "main"
schema = "threat_graph_analytics"
model_name = "threat_graph_analytics_agent"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, tags = {"endpointSource": "playground"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See [docs](https://docs.databricks.com/generative-ai/deploy-agent.html) for details
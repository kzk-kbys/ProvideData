import langchain_core.prompts
import langchain.agents
import mcp
import langchain_mcp_adapters.tools
import asyncio
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

async def main():

    model = ChatOpenAI(model="gpt-4o", temperature=0.0)

    # MCP サーバ呼出の設定
    params = mcp.StdioServerParameters(
        command="python",
        args=["mcp_server.py"],
    )

    # MCP サーバを実行
    async with mcp.client.stdio.stdio_client(params) as (read, write):
        async with mcp.ClientSession(read, write) as session:
            await session.initialize()
            tools = await langchain_mcp_adapters.tools.load_mcp_tools(session)
            agent = create_react_agent(model, tools)
            agent_response = await agent.ainvoke({"messages": "what's (3 + 5) x 12?"})
            return agent_response
            
            
if __name__ == "__main__":
    result = asyncio.run(main())
    print(result)

    # asyncio.run(main())

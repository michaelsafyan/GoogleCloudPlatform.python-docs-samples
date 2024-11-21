# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from . import openllmetry_initialization

from langchain_google_vertexai import ChatVertexAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages.system import SystemMessage
from langchain.chains.sequential import SimpleSequentialChain

# Parameter supplied to OpenLLMetry initialization.
APP_NAME = 'GoogleCloudOpenLLMetryExample'


def define_and_run_ordinary_genai_app():
    """The normal code you would otherwise write.
    
    This example happens to use LangChain and VertexAI/Gemini,
    but it should work with whatever framework you use as long
    as it is supported by OpenLLMetry (with regard to using
    Google Cloud Observability for observing GenAI workloads).
    """
    llm = ChatVertexAI(model='gemini-1.5-flash')
    dnd_scenario_generation_prompt = ChatPromptTemplate([
        SystemMessage(content="""
          You are a sci-fi and fantasy afficionado who loves to
          play Dungeons and Dragons. You have been made a
          Dungeon Master and need to come up with a fantasy
          setting, villain, list of character names, and
          plot line for your upcoming DnD game.

          Please jot down:

           1. A place name.
           2. A villain name.
           3. A two-line plot description.
        """)
    ])
    dnd_scenario_generation_prompt | llm()


def main():
    """The main function that starts this demo."""
    # Invoke OpenLLMetry's SDK to monkey-patch the LangChain code
    # and inject instrumentation into it. This should also work with
    # other agent and LLM frameworks supported by OpenLLMetry.
    openllmetry_initialization.init_openllmetry(APP_NAME)

    # Create a standard GenAI app as per usual.
    define_and_run_ordinary_genai_app()


if __name__ == '__main__':
    main()
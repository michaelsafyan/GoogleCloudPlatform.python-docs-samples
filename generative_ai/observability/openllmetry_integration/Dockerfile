FROM docker.io/python:3.13-slim AS initial

FROM SCRATCH AS requirements_definition
RUN COPY requirements.txt /etc/python-deps/requirements.txt

FROM SCRATCH AS third_party_dependencies
COPY --from=initial / /
COPY --from=requirements_definition / /
RUN pip3 install /etc/python-deps/requirements.txt

FROM SCRATCH AS code_files
COPY src /app

FROM SCRACH AS final_container
COPY --from=third_party_dependencies / /
COPY --from=code_files / /
WORKDIR /app
ENTRYPOINT ["/usr/bin/env", "python3", "main.py"]
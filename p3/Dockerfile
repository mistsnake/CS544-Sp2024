# Create image based on python
FROM python:3.9

# Install libraries
RUN pip3 install grpcio==1.60.1 grpcio-tools==1.60.1 --break-system-packages

# Copy necessary files into container's working dir
COPY client.py .
COPY server.py .
COPY client.py .
COPY mathdb.proto . 

# Build .proto
RUN python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. mathdb.proto

# Make port 5440 available
EXPOSE 5440

# Run server when container is launched
CMD ["python3", "server.py"]

# Select OS
FROM ubuntu:22.04

# Install necessary dependencies
RUN apt update
RUN apt-get install sudo
RUN sudo apt update
RUN sudo apt-get install unzip
RUN sudo apt-get install wget -y

# Copy shell script into container's working dir
COPY count.sh .

# In case any permission issues arise
RUN chmod a+rwx count.sh

# Run script
CMD ./count.sh

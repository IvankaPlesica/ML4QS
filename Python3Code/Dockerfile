FROM ubuntu:20.04
FROM python:3.8.8

RUN apt-get update
RUN apt-get install sudo
RUN apt-get install git -y

COPY requirements.txt /src/requirements.txt
COPY requirements.txt /src/requirements_git.txt

RUN apt-get install python3-pip -y
RUN pip3 install pip --upgrade
RUN pip3 install Cython

WORKDIR /root
RUN python3 --version


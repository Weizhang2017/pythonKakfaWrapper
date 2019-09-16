### A wrapper for [kafka-python](https://kafka-python.readthedocs.io/en/master/usage.html)

#### Installation

Install using pip:
```shell
pip install kafkaPythonWrapper
```
Install from Github:
```shell
git clone https://github.com/Weizhang2017/pythonKakfaWrapper
cd pythonKakfaWrapper 
python setup.py install
```


#### CLI

###### Usage
```shell
usage: kafkaPython [-h] --type type --topic topic [--group_id group_id]
                   --bootstrap_server bootstrap_server [--value value]
                   [--key key]
Required arguments:
  --type              type of kafka client, consumer or producer
  --topic             specify a topic for Kafka
  --bootstrap_server  specify a bootstrap server for Kafka
  
Optional arguments:
--group_id       specify a group ID for Kafka consumer
--value          specify a value to send to Kafka
--key            specify a key to send to Kafka
```
The **client type** is either producer or consumer. If the type is producer, the flag **value** is required while **key** is optional. If the type is consumer, the flag **group_id** is required.


###### Simple Examples:

Send a message to Kafka
```shell
kafkaPython --type producer --topic test --bootstrap_server 'localhost:9092' --key test_key --value test_value
``` 

Print messages from Kafka
```shell
kafkaPython --type consumer --bootstrap_server 'localhost:9092' --topic test --group_id 1
```

#### API
###### Usage
Send the output of a function to Kafka
```python
from kafkaPythonWrapper import MessageSender

message_sender = MessageSender(topic='test')

@message_sender.send_sync()
def produce_message():
    key, value = 'email_address', 'wzhang@leadbook.com'
    print(f'{key}: {value}')
    yield key, value
```

Receive and print messages from Kafka
```python
from kafkaPythonWrapper import MessageCollector

message_collector = MessageCollector(topic='test', group_id='1')

@message_collector.consume
def print_message(**kwargs):
    print(kwargs)
```

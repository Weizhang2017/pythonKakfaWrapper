from kafkaPythonWrapper import MessageSender
from kafkaPythonWrapper import MessageCollector

def test_producer():
	message_sender = MessageSender(topic='test')
	@message_sender.send_sync()
	def produce_message():
		key, value = 'email_address', 'wzhang@leadbook.com'
		print(f'{key}: {value}')
		yield key, value
	produce_message()

def test_consumer():
	message_collector = MessageCollector(topic='test', group_id='1')
	@message_collector.consume
	def print_message(**kwargs):
		print(kwargs)
	print_message()
	
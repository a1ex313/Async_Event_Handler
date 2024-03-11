import asyncio
import aio_pika
import asyncpg
from LogGenerator import generate_log_entry
import re
from Writer import (
    createdb,
    insertdata
)


async def producer(queue):
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)

        message_id = 1
        messages_count = 1000
        messages_per_second = 100
        delay = 1.0 / messages_per_second
        async with channel:
            for _ in range(messages_count):
                message = generate_log_entry()
                await channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message.__str__().encode()
                    ),
                    routing_key='messages'
                )
                message_id = message_id+1
                await asyncio.sleep(delay)
                print(f"Sent message: {message}")


def parser(message):
    pattern = re.compile("^\w{3}\s+\d{1,2}\s\d{2}:\d{2}:\d{2}\s(\w+-)+\w+\s\w.+\[\d+\]:")

    month = ""
    day = ""
    time = ""
    user = ""
    device = ""
    process = ""
    description = ""

    if pattern.match(message):
        month = re.search(r'(^\w{3})', message).group()
        day = re.search(r'(\d{1,2})', message).group()
        time = re.search(r'(\d{2}:\d{2}:\d{2})', message).group()
        line = re.search(r'(\w+-)', message).group()
        user = line[:-1]
        line = re.search(r'(-\w+)+', message).group()
        device = line[1:]
        line = re.search(r'(\w+\[\d+\]:\s)', message)
        pos = line.end()
        line = re.search(r'(\w+\[\d+\]:\s)', message).group()
        process = line[:-2]
        description = message[pos:]

    return month, day, time, user, device, process, description


async def consumer(connection, queue):
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue('messages', durable=True)
        await channel.set_qos(prefetch_count=10)
        async for message in queue:
            async with message.process():
                data = message.body.decode()
                print(f"Consumer Received message: {data}")

                mes = data.__str__()
                month, day, time, user, device, process, description = parser(mes)
                await save_message_to_db(month, day, time, user, device, process, description)
                '''
                createdb()
                if not month:
                    print("Неверные данные")
                else:
                    insertdata(month, day, time, user, device, process, description)                
                '''


async def save_message_to_db(month, day, time, user, device, process, description):
    conn = await asyncpg.connect(user='root', password='1111', database='db_logs2', host='localhost')
    await conn.execute('''
                       INSERT INTO logs (month, day, time, user_name, device, process, description)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)''',
                       month, day, time, user, device, process, description)
    await conn.close()

async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        queue = await connection.channel()
        await queue.set_qos(prefetch_count=10)
        tasks = []

        tasks.append(asyncio.create_task(producer(queue)))
        tasks_count = 3
        for _ in range(tasks_count):
            tasks.append(asyncio.create_task(consumer(connection, queue)))


        await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())


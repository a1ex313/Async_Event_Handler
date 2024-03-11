import random
import time

months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

users = ['alex', 'john', 'logan', 'mike', 'victor']

devices = ['laptop', 'phone', 'tablet', 'desktop', 'server']

processes = ['dbus-daemon', 'gnome-shell', 'firefox', 'chrome', 'python', 'opera', 'jdk']

descriptions = ['Successfully activated service', 'Failed to start', 'Connection error', 'Permission denied',
                'Invalid argument']


def generate_log_entry():
    month = random.choice(months)
    user = random.choice(users)
    day = random.randint(1, 31)
    time = f"{random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"
    device = random.choice(devices)
    process = random.choice(processes)
    description = random.choice(descriptions)
    num_process = random.randint(1, 3350)

    return f"{month}  {day} {time} {user}-{device} {process}[{num_process}]: {description}"


def produce_logs():
    messages_per_second = 100
    delay = 1.0 / messages_per_second
    logs = []
    for _ in range(messages_per_second):
        logs.append(generate_log_entry())
        time.sleep(delay)
        return logs
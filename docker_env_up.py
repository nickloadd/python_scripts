import subprocess
import time

# Data stotages and apllications list with "started key" string in logs
datastotages = [["aerospike", "NODE-ID"], ["zookeeper", "binding to port"], ["kafka", "[KafkaServer id=1] started"], ["mysql", "ready for connections"], ["clickhouse", ""]]
applications = [["app1","Started app1"], ["app2","Starting app2"]]
start_limit = 15

# Function to read containers logs                    
def get_logs(service):
    logs = ""
    proc = subprocess.Popen(["docker-compose", "logs", service], stdout=subprocess.PIPE)
    while True:
        line = proc.stdout.readline().decode("utf-8")
        if not line:
            break
        logs += line
    return logs

# Function to start data storage containers
def start_datastorages():
    for service in datastotages:
        counter = 0
        subprocess.run(["docker-compose", "up", "-d", service[0]])
        # Loop for check successful start container via "started key"
        while service[1] not in get_logs(service[0]):
                    counter = counter + 1
                    if (counter != start_limit) :
                        print("Starting " + service[0] + " service...")
                        time.sleep(5)
                    else:
                        print("Starting limit is reached for " + service[0] + " service is reached ")
                        raise SystemExit
                        #subprocess.run(["docker-compose", "down", "-v"])
                        #print("Destroy docker env")

# Function to start application containers
def start_applications():
    for app in applications:
        counter = 0
        subprocess.run(["docker-compose", "up", "-d", app[0]])
        while app[1] not in get_logs(app[0]):
                    counter = counter + 1
                    if (counter != start_limit) :
                        print("Starting " + app[0] + " application...")
                        time.sleep(15)
                    else:
                        print("Starting limit is reached for " + app[0] + " application is reached ")
                        raise SystemExit
                        #subprocess.run(["docker-compose", "down", "-v"])
                        #print("Destroy docker env")
    time.sleep(15)
    print("Docker env is up and ready for autotest...")

# Function to run autotest in docker env
def run_tests():
    subprocess.run(["mvn", "test"])
    time.sleep(5)

# Function to Stop and remove containers
def cleanup_containers():
    subprocess.run(["docker-compose", "down", "-v"])

def main():
    start_datastorages()
    start_applications()
    run_tests()
    cleanup_containers()

if __name__ == "__main__":
    main()

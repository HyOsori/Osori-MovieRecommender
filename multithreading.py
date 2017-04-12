import threading

def testfunction(a):
    print(a)

"""
multithreading is a function to perform multiple queries at once
n is the parameter to choose the limit of thread that can run at once
arguments are the complete list of parameters that you want to pass to
func which is the function you want to run parallely
"""

def multithreading(func, arguments, n=10):#since multiple arguments may be passed every item in args is a list
    threadsStarted = []
    for i in range(0, len(arguments), n):
        threadsStarted = []
        for j in range(i, min(len(arguments), n + i)):
            threadObj = threading.Thread(target=func, args=arguments[j])
            threadsStarted.append(threadObj)
            threadObj.start()
        for thread in threadsStarted:
            thread.join()
        print("Continuing")

if __name__ == '__main__':
    multithreading(testfunction, [[x] for x in range(100)])

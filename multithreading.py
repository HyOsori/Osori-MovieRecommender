import threading


# since multiple arguments may be passed every item in args is a list
def multi_threading(func, arguments, n=10):

    for i in range(0, len(arguments), n):

        thread_objects = []
        for j in range(i, min(len(arguments), n + i)):

            thread = threading.Thread(target=func, args=arguments[j])
            thread_objects.append(thread)
            thread_objects.append(thread)
            thread.start()

        for thread in thread_objects:
            thread.join()

        print("Continuing")


def test_function(a):
    print(a)

if __name__ == '__main__':
    multi_threading(test_function, [[x] for x in range(100)])

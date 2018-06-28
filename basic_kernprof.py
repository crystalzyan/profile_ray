import ray
import time

# Python native timing functionality
def time_this(f):
	def timed_wrapper(*args, **kw):
		start_time = time.time()
		result = f(*args, **kw)
		end_time = time.time()

		print('| func:%r args:[%r, %r] took: %2.4f seconds |' % \
          (f.__name__, args, kw, end_time - start_time))
		return result
	return timed_wrapper


# Remote slow functions
@ray.remote
def func():
	time.sleep(0.5)

@ray.remote
def func2():
	time.sleep(0.2)


# Ray syntax examples for comparison
@profile
#@time_this
def ex1():
	list1 = []
	for i in range(5):
		list1.append(ray.get(func.remote()))

@profile
#@time_this
def ex2():
	list2 = []
	for i in range(5):
		list2.append(func.remote())
	ray.get(list2)

@profile
#@time_this
def ex3():
	list3 = []
	for i in range(5):
		func2.remote()
		list3.append(func.remote())
	ray.get(list3)


# Prompt user to use Python timing functionality
def main():
	ray.init()

	split = -1
	while split != 0 and split != 1:
		split = int(input('Enter 0 to use python time module, 1 otherwise (for external timing applications):'))

	if split == 0:
		time_this(ex1)()
		time_this(ex2)()
		time_this(ex3)()
	elif split == 1:
		ex1()
		ex2()
		ex3()

if __name__ == "__main__":
	main()

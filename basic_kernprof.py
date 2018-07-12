import ray
import time
import cProfile

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

# Changed to be local
def func2():
	time.sleep(0.3)


# Ray syntax examples for comparison
#@profile
#@time_this
def ex1():
	list1 = []
	for i in range(5):
		list1.append(ray.get(func.remote()))

#@profile
#@time_this
def ex2():
	list2 = []
	for i in range(5):
		list2.append(func.remote())
	ray.get(list2)

#@profile
#@time_this
def ex3():
	list3 = []
	for i in range(5):
		func2()
		list3.append(func.remote())
	ray.get(list3)


# Added actor
@ray.remote
class Sleeper(object):
  def __init__(self):
      self.sleepValue = 0.5

  def actor_func(self):
      time.sleep(self.sleepValue)

def ex4bad():
	actor_example = Sleeper.remote()

	five_results = []
	for i in range(5):
		five_results.append(actor_example.actor_func.remote())
	ray.get(five_results)

def ex4():
	five_actors = [Sleeper.remote() for i in range(5)]

	five_results = []
	for actor_example in five_actors:
		five_results.append(actor_example.actor_func.remote())
	ray.get(five_results)



# Prompt user to use Python timing functionality
def main():
	ray.init()

	split = -1
	while split != 0 and split != 1 and split != 2 and split != 3:
		print('Enter an integer:\n' + \
			  '0 to use python time module,\n' + \
			  '1 for vanilla execution such as for external timing applications,\n' + \
			  '2 for python cProfile module,\n' + \
			  '3 for cProfile module on Actor:')
		split = int(input(''))

	if split == 0:
		time_this(ex1)()
		time_this(ex2)()
		time_this(ex3)()
	elif split == 1:
		ex1()
		ex2()
		ex3()
	elif split == 2:
		cProfile.run('ex1()')
		cProfile.run('ex2()')
		cProfile.run('ex3()')
	elif split == 3:
		cProfile.run('ex4bad()')
		print('...')
		print('...')
		print('...')
		cProfile.run('ex4()')

	hang = input('Examples finished executing. Press enter to exit:')

if __name__ == "__main__":
	main()

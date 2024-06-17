import matplotlib.pyplot as plt
from random import choice

class RandomWalk:
    """ A class to handle random walks """

    def __init__(self, num_points=5000):
        """ Initialize attributes """
        self.num_points = num_points
        self.x_values = [0]
        self.y_values = [0]

    def fill_walk(self):
        """ Calculate random values """

        while len(self.x_values) < self.num_points:
            x_direction = choice([1, -1])
            x_distance = choice(range(0, 4))
            x_step = x_direction * x_distance

            y_direction = choice([1, -1])
            y_distance = choice(range(0, 4))
            y_step = y_direction * y_distance

            if x_step == 0 and y_step == 0:
                continue

            x = self.x_values[-1] + x_step
            y = self.y_values[-1] + y_step

            self.x_values.append(x)
            self.y_values.append(y)


while True:
    rwk = RandomWalk()
    rwk.fill_walk()

    plt.style.use('classic')
    fig, ax = plt.subplots(figsize=(15, 9))

    point_numbers = range(rwk.num_points)

    ax.scatter(0, 0, c='green', edgecolors='none', s=100)
    ax.scatter(rwk.x_values, rwk.y_values, c=point_numbers, cmap=plt.cm.Blues, edgecolors='none', s=15)
    ax.scatter(rwk.x_values[-1], rwk.y_values[-1], c='red',  edgecolors='none', s=100)

    #ax.get_xaxis().set_visible(False)
    #ax.get_yaxis().set_visible(False)

    plt.show()

    keep_running = input("Make another walk (y/y)? ")

    if keep_running == 'n':
        break

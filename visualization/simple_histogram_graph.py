from random import randint
from plotly.graph_objs import Bar, Layout
from plotly import offline

class Die:
    """ A class representing a single die """

    def __init__(self, num_sides=6):
        """ Assume a 6 sided dice """
        self.num_sides = num_sides

    def roll(self):
        """ Roll the dice mofo """
        return randint(1, self.num_sides)

die = Die()

results = []
for roll_num in range(100):
    result = die.roll()
    results.append(result)

print(results)

frequencies = []
for value in range(1, die.num_sides+1):
    frequency = results.count(value)
    frequencies.append(frequency)

print(frequencies)

x_values = list(range(1, die.num_sides+1))
data = [Bar(x=x_values, y=frequencies)]

x_axis_config = {'title': 'Result'}
y_axis_config = {'title': 'Frequency'}
my_layout = Layout(title='Results', xaxis=x_axis_config, yaxis=y_axis_config)
offline.plot({'data': data, 'layout': my_layout}, filename='example.html')

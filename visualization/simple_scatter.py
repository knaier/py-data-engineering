import matplotlib.pyplot as plt

x_values = range(1, 1001)
y_values = [x**2 for x in x_values] # list comprehension is cool, calculate automatically y for x

fig, ax = plt.subplots()
ax.scatter(x_values, y_values, s=100)

ax.set_title("Square Numbers", fontsize=24)
ax.set_xlabel("Value", fontsize=14)
ax.set_ylabel("Square of Value", fontsize=14)

ax.tick_params(axis='both', labelsize=14)

plt.style.use('seaborn')
plt.show()

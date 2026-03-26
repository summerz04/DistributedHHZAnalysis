import matplotlib.pyplot as plt
import numpy as np

print('Hello from container!')

x = np.linspace(0, 100, 100)
y = x**2
plt.plot(x, y, label="$y=x^2$")
plt.legend()
plt.savefig("test.png")

print('Bye bye container!')
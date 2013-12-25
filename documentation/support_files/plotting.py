import numpy as np
import matplotlib.pyplot as plt

# word growth
x_axis = np.arange(0, 11, 1)
#y_axis = np.arange(0, 2500, 500)
chavez = np.array([5, 197, 301, 352, 484, 591, 723, 963, 1178, 1431, 1976])
capriles = np.array([5, 241, 332, 349, 506, 630, 868, 1051, 1411, 1641, 2286])
plt.figure(1)
plt.plot(x_axis, chavez, 'r-', label='chavez vocabulary', linewidth=2)
plt.plot(x_axis, capriles, 'b-', label='capriles vocabulary', linewidth=2)
plt.xlabel('Iterations')
plt.ylabel('Size of Un-normalized Vocabulary')
plt.grid('on')
plt.legend(loc='lower right')
plt.savefig('./WordGrowth.png')
print '.....wordGrowth plotted'
####################################################################################
# recall
N = 8
ind = np.arange(N)
width = 0.35

seed = [142909, 231292, 31876, 487698, 12938, 72941, 11278, 40091]
psl = [234712, 310665, 83941, 716928, 26357, 121766, 17788, 123017]

fig, ax = plt.subplots()
rects1 = ax.bar(ind, seed, width, color='r')
rects2 = ax.bar(ind + width, psl, width, color='b')

ax.set_xlabel('Elections')
ax.set_ylabel('Number of tweets used for prediction')
ax.set_xticks(ind + width)
ax.set_xticklabels(('MX', 'VE_oct7', 'EC', 'VE_apr15', 'PY', 'CL_nov17', 'HN', 'CL_Dec15'))
plt.grid('on')
ax.legend((rects1[0], rects2[0]), ('Seed Vocab', 'PSL Vocab'))
plt.savefig('./Recall.png')
print '....recall plotted'

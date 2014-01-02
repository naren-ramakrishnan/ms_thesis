import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
'''
# word growth
x_axis = np.arange(0, 11, 1)
#y_axis = np.arange(0, 2500, 500
chavez = np.array([5, 19, 30, 35, 48, 59, 72, 96, 117, 143, 197])
capriles = np.array([5, 24, 33, 34, 50, 63, 86, 105, 141, 164, 228])
plt.figure(1)
plt.plot(x_axis, chavez, 'r-', label='chavez vocabulary', linewidth=2)
plt.plot(x_axis, capriles, 'b-', label='capriles vocabulary', linewidth=2)
plt.xlabel('Iterations')
plt.ylabel('Size of Vocabulary')
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
plt.figure(2)
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
'''
########################################################################################
# hashTag time seriers
x_axis = np.arange(0, 11, 1)
#elmundoconchavez
word1 = np.array([0, 0.851, 0.821, 0.76543, 0.743, 0.8156, 0.7153, 0.740, 0.8193, 0.7789, 0.8056]) 
#beatles
word2 = np.array([0, 0.3145, 0.14, 0, 0, 0, 0, 0, 0, 0, 0])
#facebook
word3 = np.array([0, 0, 0, 0.15, 0, 0.14, 0.1145, 0, 0, 0, 0])
#univistaconchavez
word4 = np.array([0, 0.32, 0.47, 0.60, 0.52, 0.45, 0.36, 0.389, 0.412, 0.40, 0.391])
#vivachavez_oct7
word5 = np.array([0, 0.43, 0.34, 0.52, 0.47, 0.3, 0.36, 0.76, 0.65, 0.56, 0.55])
#vivachavez_apr15
word6 = np.array([0, 0, 0, 0.48, 0.55, 0.33, 0.31, 0.39, 0.36, 0.35, 0.31])

fig3 = plt.figure(3)
plt311 = fig3.add_subplot(311)
plt312 = fig3.add_subplot(312)
plt313 = fig3.add_subplot(313)

plt311.plot(x_axis, word1, 'r-', label='elmundoconchavez')
plt311.plot(x_axis, word4, 'g-', label='univistaconchavez')
plt311.set_ylim([0,1])
plt311.legend(bbox_to_anchor=(1.12, 0.3))

plt312.plot(x_axis, word2, 'r-', label='beatles')
plt312.plot(x_axis, word3, 'g-', label='facebook')
plt312.set_ylim([0,1])
plt312.set_xlim([0,10])
plt312.legend(bbox_to_anchor=(1.12, 0.3))

plt313.plot(x_axis, word5, 'r-', label='vivachavez oct_7')
plt313.plot(x_axis, word6, 'g-', label='vivachavez apr_15')
plt313.set_ylim([0,1])
plt313.legend(bbox_to_anchor=(1.12, 0.3))

fig3.text(0.5, 0.04, 'iterations', ha='center', va='center')
fig3.text(0.06, 0.5, 'hashTag weight', ha='center', va='center', rotation='vertical')

plt.savefig('./hashTagTimeSeries.png')
print '.....hashTag Time Series Plotted'

import random
import matplotlib.pyplot as plt
from tqdm import tqdm


class Reservoir:
    def __init__(self, k):
        self.k = k
        self.sampled = []
        self.idx = 0

    def put(self, item):

        if self.idx < self.k:
            self.sampled.append(item)
            self.idx += 1
            return item # 추가

        else:
            r = random.randint(0, self.idx)

            if r < self.k:
                self.sampled[r] = item
                self.idx += 1
                return item # 추가

        self.idx += 1
        return None # 추가


class ReservoirWithReplacement:
    def __init__(self, k):
        self.k = k
        self.sampled = [-1 for _ in range(self.k)]
        self.res = [Reservoir(1) for _ in range(self.k)]

    def put(self, item):

        for i in range(self.k): # k=100
            replace = self.res[i].put(item)

            if replace != None: # random을 통해 교체된 경우
                self.sampled[i] = replace


# main
result = []
result_rp = []
for _ in tqdm(range(10000)): 
    reservoir = Reservoir(100)
    reservoir_rp = ReservoirWithReplacement(100)

    for x in range(1000): # k값 임의 지정
        reservoir.put(x)
        reservoir_rp.put(x)       
    
    for i in range(100):
        result.append(reservoir.sampled[i])
        result_rp.append(reservoir_rp.sampled[i])

# plotting
fig, axs = plt.subplots(1,2,constrained_layout=True)

axs[0].plot([i for i in range(1000)], [result.count(i) for i in range(1000)], 'r.')
axs[0].set_title("Reservoir Sampling")
axs[0].set_xlabel("sampled number")
axs[0].set_ylabel("num of sampled number")

axs[1].plot([i for i in range(1000)], [result_rp.count(i) for i in range(1000)], 'b.')
axs[1].set_title("Reservoir with Replacement Sampling")
axs[1].set_xlabel("sampled number")
axs[1].set_ylabel("num of sampled number")

plt.show()

#23초
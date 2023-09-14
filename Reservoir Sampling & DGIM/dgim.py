"""
DGIM Algorithm을 응용하여 0에서 15 사이의 정수 스트림에서 최근 k개 숫자의 합을 계산할 수 있는 알고리즘을 수업시간에 언급했던 두 가지 방식으로 구현하세요.
10000개의 0에서 15 사이의 정수가 임의의 순서로 입력되는 스트림에서 최근 k=(1~2000)개의 수에 대해, 실제 합, 첫번째 방법으로 구한 합, 두번째 방법으로 구한 합을 계산하여, 어느 알고리즘이 더욱 정확한지 확인하세요. (예: 가로 축 = k, 세로 축 = 합이 되도록 plotting 하기)
"""

import random
from tqdm import tqdm
import matplotlib.pyplot as plt

class Bucket:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __repr__(self):
        return f"({self.start}, {self.end})"

# bitstream DGIM
class DGIM:
    def __init__(self):
        self.bucket_tower = [[]]
        self.ts = 0

    def put(self, bit):

        if bit == 1:
            self.bucket_tower[0].insert(0, Bucket(self.ts, self.ts))

            layer = 0
            while len(self.bucket_tower[layer]) > 2:
                if len(self.bucket_tower) < layer+2:
                    self.bucket_tower.append([])

                b1 = self.bucket_tower[layer].pop() # 2번은 여기서 2**n보다 크면 하나만 pop함. 그 외의경우엔 두개 pop하고 ts합쳐주고.
                b2 = self.bucket_tower[layer].pop()
                b1.end = b2.end

                self.bucket_tower[layer+1].insert(0, b1)
                layer += 1

        self.ts += 1

    def count(self, k):
        s = self.ts - k

        cnt = 0

        for layer, buckets in enumerate(self.bucket_tower):
            for bucket in buckets:
                if s <= bucket.start:
                    cnt += (1 << layer)
                elif s <= bucket.end:
                    cnt += (1 << layer) * (bucket.end - s + 1) // (bucket.end - bucket.start + 1)
                    return cnt
                else:
                    return cnt

        return cnt


"""
1. 모든 정수가 m 비트 이하라면(32 bit)
각 m개의 비트를 별도의 스트림으로 보고, 각 스트림에 DGIM 알고리즘 적용
합 = 각 스트림에 1, 2, 4, 8 등을 곱하여 더하기
"""
class IntStreamDGIM1:
    def __init__(self):
        self.dgims = [DGIM() for _ in range(4)] # dgims[0]이 2^3자리, dgims[3]이 2^0 자리.

    def put(self, integer):
        bin = '{0:04b}'.format(integer) # 입력 받은 10진수 정수를 4자리 이진수로 바꿔줌(왜냐하면 입력이 0~15)
        for i in range(len(self.dgims)): 
            self.dgims[i].put(int(bin[i])) # 이진수의 각 자리에 기존에 구현한 bit stream DGIM 알고리즘 적용

    def count(self, k):
        cnt = 0

        for i in range(len(self.dgims)):
            cnt += self.dgims[i].count(k) * (1 << (3-i)) # 이진수의 각 자리별 1의 개수를 예측하고, 자리 값을 곱하여 줌. 

        return cnt



"""
2. 영역에 부분 합 저장하기
영역의 크기를 b라 할 때, 영역 내 값의 합이 2**b 이하가 되도록 유지
"""
class IntStreamDGIM2:
    def __init__(self):
        self.bucket_tower = [[]] # 버킷의 time stamp를 저장할 배열
        self.bucket_sum = [[]] # 버킷의 부분합을 저장할 배열
        self.ts = 0

    def put(self, integer):
        self.bucket_tower[0].insert(0, Bucket(self.ts, self.ts)) # 입력으로 들어온 버킷 맨 앞에 넣기
        self.bucket_sum[0].insert(0, integer)

        layer = 0
        while len(self.bucket_tower[layer]) > 2:
            if len(self.bucket_tower) < layer+2: # 위 layer가 없는 경우 생성
                self.bucket_tower.append([])
                self.bucket_sum.append([])

            if self.bucket_sum[layer][-1] + self.bucket_sum[layer][-2] > (2 << layer): # 버킷이 3개가 된 layer의 뒤의 두 버킷의 합이 다음 layer의 기댓값보다 큰 경우 하나의 버킷만 올려줌.
                b1 = self.bucket_tower[layer].pop()
                s1 = self.bucket_sum[layer].pop()

                self.bucket_tower[layer+1].insert(0, b1)
                self.bucket_sum[layer+1].insert(0, s1)
            else: # 두 버킷의 합이 다음 layer의 기댓값 이하인 경우 둘 다 올려줌
                b1 = self.bucket_tower[layer].pop()
                b2 = self.bucket_tower[layer].pop()
                s1 = self.bucket_sum[layer].pop()
                s2 = self.bucket_sum[layer].pop()

                b1.end = b2.end # 두 버킷의 time stamp 합쳐줌
                s = s1 + s2 # 두 버킷의 부분합을 합쳐줌


                self.bucket_tower[layer+1].insert(0, b1) # 위 layer에 넣어줌
                self.bucket_sum[layer+1].insert(0, s)
            layer += 1

        self.ts += 1
    
    def count(self, k):
        s = self.ts - k

        cnt = 0

        for zipped in zip(self.bucket_tower, self.bucket_sum):
            buckets = zipped[0]
            sums = zipped[1]

            for i in range(len(buckets)):
                if s <= buckets[i].start: # 해당 버킷을 다 포함하는 경우
                    cnt += sums[i]
                elif s <= buckets[i].end: # 해당 버킷의 일부만 포함하는 경우
                    cnt += round(sums[i] * (buckets[i].end - s + 1) // (buckets[i].end - buckets[i].start + 1))
                    return cnt
                else:
                    return cnt

        return cnt


# dgim = DGIM()
intstreamdgim1 = IntStreamDGIM1()
intstreamdgim2 = IntStreamDGIM2()

intstream = []
for _ in tqdm(range(10000)): 
    r = random.randint(0, 15)
    intstream.append(r)

    intstreamdgim1.put(r)
    intstreamdgim2.put(r)    

real_sum = []
intstreamdgim1_sum = []
intstreamdgim2_sum = []

for k in range(1,2001):
    real_sum.append(sum(intstream[-k:]))
    intstreamdgim1_sum.append(intstreamdgim1.count(k))
    intstreamdgim2_sum.append(intstreamdgim2.count(k))
    print(f'k: {k}   real: {sum(intstream[-k:])}   method1: {intstreamdgim1.count(k)}   method2: {intstreamdgim2.count(k)}')
    #print(k, sum(intstream[-k:]), intstreamdgim1.count(k), intstreamdgim2.count(k))


# plotting
plt.plot([k for k in range(1, 2001)], real_sum, label='real')
plt.plot([k for k in range(1, 2001)], intstreamdgim1_sum, label='method1')
plt.plot([k for k in range(1, 2001)], intstreamdgim2_sum, label='method2')
plt.xlabel('k')
plt.ylabel('sum of recent k')

plt.legend()
plt.show()

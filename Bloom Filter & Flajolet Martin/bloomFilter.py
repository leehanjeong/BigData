import mmh3
import math
import random
import matplotlib.pyplot as plt
import time
import sys


class BloomFilter:
    def __init__(self, capacity, fp_prob):
        self.capacity = capacity # B == m
        self.fp_prob = fp_prob
        self.bit_array = 0
        self.n_bits = math.ceil(-math.log(fp_prob, math.e) * capacity / (math.log(2, math.e) ** 2)) # S == n
        self.n_hashes = math.ceil(self.n_bits / capacity * math.log(2, math.e))
        self.seeds = [random.randint(0, 999999) for i in range(self.n_hashes)]


    def put(self, item):
        for i in range(self.n_hashes):
            pos = mmh3.hash(item, self.seeds[i]) % self.n_bits
            # print(mmh3.hash(item, self.seeds[i]), self.n_bits, pos)
            self.bit_array |= (1 << pos)
        

    def test(self, item):
        for i in range(self.n_hashes):
            pos = mmh3.hash(item, self.seeds[i]) % self.n_bits

            if self.bit_array & (1 << pos) == 0:
                return False
            
        return True

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Needs more argument.")

    case = int(sys.argv[1])

    # 0. BF의 false positive 비율 잘 나오는지 확인
    if case == 0:
        bloom = BloomFilter(1000, 0.1)
        true_list = []
        false = 0
        false_positive = 0

        for i in range(1, 1001): # '1'~'1000' 입력
            bloom.put(str(i))
            true_list.append(str(i))

        for i in range(1, 100001): # 100000번 실험
            r = str(random.randint(1, 10001)) # '1'~'10000' 테스트
            
            ans = bloom.test(r)
            
            if r not in true_list:
                false += 1
                if ans == True:
                    false_positive += 1
                    print("input false positive rate:", 0.1)
                    print("false positive rate:", false_positive / false)
                    # print("expected false positive rate:", (1/2) ** bloom.n_hashes) --> 이거는 round, ceil 같은 작업 들어가서 다르게 나오는 것 같다.

    # 1. capacity 값 바꾸기
    elif case == 1:
        x = []
        y = []
        fp = 0.1
        for c in range(1, 2002, 100):
            x.append(c)
            bloom = BloomFilter(c, fp)
            true_list = []
            false = 0
            false_positive = 0

            for i in range(1, 1001): # '1'~'1000' 입력
                bloom.put(str(i))
                true_list.append(str(i))

            for i in range(1, 100001): # 100000번 실험
                r = str(random.randint(1, 10001)) # '1'~'10000' 테스트
                
                ans = bloom.test(r)
                
                if r not in true_list:
                    false += 1
                    if ans == True:
                        false_positive += 1
                        print("input false positive rate:", fp)
                        print("false positive rate:", false_positive / false)
                        # print("expected false positive rate:", (1/2) ** bloom.n_hashes) --> 이거는 round, ceil 같은 작업 들어가서 다르게 나오는 것 같다.
            
            y.append(false_positive / false)       
            print('n_bits:', bloom.n_bits, 'n_hashes:', bloom.n_hashes) 

        plt.title('FP rate per Capacity')
        plt.plot(x, y, label='fp rate')
        plt.plot(x, [fp for _ in range(len(x))], label='expected fp rate')
        plt.xlabel('Capacity')
        plt.ylabel('FP rate')
        plt.legend()
        plt.show()

    # 2. fp_prob 값 바꾸기
    elif case == 2:
        x = []
        y = []
        hashes = []
        bits = []
        fps = []
        fp = 0.005
        while fp <= 1:
            x.append(fp)
            bloom = BloomFilter(1000, fp)
            true_list = []
            false = 0
            false_positive = 0

            for i in range(1, 1001): # '1'~'1000' 입력
                bloom.put(str(i))
                true_list.append(str(i))

            for i in range(1, 100001): # 100000번 실험
                r = str(random.randint(1, 10001)) # '1'~'10000' 테스트
                
                ans = bloom.test(r)
                
                if r not in true_list:
                    false += 1
                    if ans == True:
                        false_positive += 1
                        # print("input false positive rate:", fp)
                        # print("false positive rate:", false_positive / false)
                        # print("expected false positive rate:", (1/2) ** bloom.n_hashes) --> 이거는 round, ceil 같은 작업 들어가서 다르게 나오는 것 같다.
            

            y.append(false_positive / false)       
            hashes.append(bloom.n_hashes)
            bits.append(bloom.n_bits)
            fps.append(fp)
            #print('n_bits:', bloom.n_bits, 'n_hashes:', bloom.n_hashes) 
            print(fp)

            fp += 0.01

        fig, axs = plt.subplots(1,3,constrained_layout=True)

        axs[0].plot(x, y, label='fp rate')
        axs[0].plot(x, fps, label='expected fp rate')
        axs[0].set_title("FP rate per Expected FP rate")
        axs[0].set_xlabel("Expected FP rate")
        axs[0].set_ylabel("FP rate")
        axs[0].legend()

        axs[1].plot(x, hashes)
        axs[1].set_title("Num of hashes per Expected FP rate")
        axs[1].set_xlabel("Expected FP rate")
        axs[1].set_ylabel("Num of hashes")

        axs[2].plot(x, bits)
        axs[2].set_title("Num of bits per Expected FP rate")
        axs[2].set_xlabel("Expected FP rate")
        axs[2].set_ylabel("Num of bits")

        plt.show()

    # 3. capacity 별 hash 속도 
    elif case == 3:
        x = []
        capacities = []
        n_bits = []
        n_hashes = []
        times = []
        fp = 0.1
        for c in range(10000, 300001, 20000):

            capacities.append(c)
            bloom = BloomFilter(c, fp)
            true_list = []
            false = 0
            false_positive = 0

            for i in range(1, 100001): # '1'~'1000' 입력
                bloom.put(str(i))
                true_list.append(str(i))

            start = time.time()
            for i in range(1, 1001): # 100000번 실험
                r = str(random.randint(1, 200001)) # '1'~'10000' 테스트
                
                ans = bloom.test(r)
            end = time.time()

            n_bits.append(bloom.n_bits)
            n_hashes.append(bloom.n_hashes)
            times.append(end - start)
                    
            print('n_bits:', bloom.n_bits, 'n_hashes:', bloom.n_hashes) 
            
            print("n_bits =", c, "time = ", f"{end - start:.5f} sec")
            # print("n_bits =", c, "time = ", end-start)

        fig, axs = plt.subplots(1,3,constrained_layout=True)

        axs[0].plot(capacities, n_bits, label='fp rate')
        axs[0].set_title("Number of bits per Capacity")
        axs[0].set_xlabel("Capacity")
        axs[0].set_ylabel("Number of bits")

        axs[1].plot(capacities, times)
        axs[1].set_title("Time per Expected Capacity")
        axs[1].set_xlabel("Capacity")
        axs[1].set_ylabel("Time")

        axs[2].plot(capacities, n_hashes)
        axs[2].set_title("Num of hashes per Capacity")
        axs[2].set_xlabel("Capacity")
        axs[2].set_ylabel("Num of hashes")

        plt.show()

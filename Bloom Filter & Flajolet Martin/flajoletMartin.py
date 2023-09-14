import mmh3
import math
import random
import matplotlib.pyplot as plt

class FlajoletMartin1:
    def __init__(self, domain_size):
        self.R = 0
        self.domain_size = domain_size # N
        self.n_bits = math.ceil(math.log2(domain_size)) # 비트 수
        self.mask = (1 << self.n_bits) - 1 # 다 1
        self.seed = random.randint(0, 9999999)

    def put(self, item):
        h = mmh3.hash(item, self.seed) & self.mask
        r = 0

        if h == 0:
            return
        while h & (1 << r) == 0:
            r += 1

        if r > self.R:
            self.R = r


    def size(self):
        return 2 ** self.R

class FlajoletMartin2:
    def __init__(self, domain_size):
        self.bit_array = 0
        self.domain_size = domain_size # N
        self.n_bits = math.ceil(math.log2(domain_size)) # 비트 수
        self.mask = (1 << self.n_bits) - 1 # 다 1
        self.seed = random.randint(0, 9999999)

    def put(self, item):
        h = mmh3.hash(item, self.seed) & self.mask
        r = 0

        if h == 0:
            return
        while h & (1 << r) == 0:
            r += 1

        self.bit_array |= (1 << r)


    def size(self):
        R = 0
        while (self.bit_array & (1 << R) != 0): # R 찾기
            R += 1
        
        return 2 ** R / 0.77351


class UpgradeFlajoletMartin2:
    def __init__(self, domain_size, num_hashes):
        self.num_hashes = num_hashes
        self.bit_array = [0 for _ in range(num_hashes)]
        self.domain_size = domain_size # N
        self.n_bits = math.ceil(math.log2(domain_size)) 
        self.mask = (1 << self.n_bits) - 1
        self.seeds = [random.randint(0, 9999999) for _ in range(self.num_hashes)]

    def put(self, item):
        for i in range(self.num_hashes):
            h = mmh3.hash(item, self.seeds[i]) & self.mask
            r = 0

            if h == 0:
                return
            while h & (1 << r) == 0:
                r += 1

            self.bit_array[i] |= (1 << r)



    def size(self, num_group):
        sizes = []
        quotient = self.num_hashes // num_group
        remainder = self.num_hashes % num_group

        for i in range(self.num_hashes):
            R = 0
            while (self.bit_array[i] & (1 << R) != 0): # R 찾기
                R += 1
            sizes.append(2 ** R / 0.77351)
        #print(sizes)
        
        mean = 0
        prev_group_size = 0
        for i in range(num_group):
            if i+1 <= remainder:
                group_size = quotient + 1
            else:
                group_size = quotient

            # 중앙값들 더해주기
            if group_size % 2 == 0:
                mean += (sizes[prev_group_size + group_size//2] + sizes[prev_group_size + group_size//2 - 1]) / 2
                #print(self.num_hashes, num_group, group_size, prev_group_size + group_size//2, "!!", (sizes[prev_group_size + group_size//2] + sizes[prev_group_size + group_size//2 - 1]) / 2)
            else:
                mean += sizes[prev_group_size + group_size//2]
                #print(self.num_hashes, num_group, group_size, prev_group_size + group_size//2, "??", sizes[prev_group_size + group_size//2])
            
            prev_group_size += group_size
        
        mean = mean / num_group
 
        return mean

if __name__ == '__main__':
    fm1 = FlajoletMartin1(10000000)
    fm2 = FlajoletMartin2(10000000)
    tset = set()

    x = []
    y = []
    z = []
    w = []

    error1 = 0
    error2 = 0


    for j in range(1000):
            item = str(random.randint(0, 9999))
            tset.add(item)
            fm1.put(item)
            fm2.put(item)

            x.append(len(tset))
            y.append(fm1.size())
            z.append(fm2.size())

            error1 += (len(tset) - fm1.size()) ** 2
            error2 += (len(tset) - fm2.size()) ** 2

    error = []
    for i in range(1, 11): # hash 개수
        fm3 = UpgradeFlajoletMartin2(10000000, i)

        error3 = [0 for _ in range(i)]
        for j in range(1000): # item 개수
            item = str(random.randint(0, 9999))
            fm3.put(item)
            
            for k in range(1, i+1): # group 개수
                w.append(fm3.size(k))
                #print('real:', x[j], 'expect:', fm3.size(k), 'n_hash:', i, 'n_group:', k)
                error3[k-1] += (x[j] - fm3.size(k)) ** 2
        
        for i in range(len(error3)):
            error3[i] = math.sqrt(error3[i]/len(tset))
        error.append(error3)
    #print(error)
                
    for i in range(10):
        plt.plot([x for x in range(1, len(error[i])+1)], error[i], '-o', label=str(i+1)+' hash')
    plt.xlabel('Num of groups')
    plt.ylabel('Error')
    plt.legend()
    plt.show()


    error1 = math.sqrt(error1/len(tset))
    error2 = math.sqrt(error2/len(tset)) 
    #error3 = math.sqrt(error3/len(tset)) 

    print("version1 error:", error1)
    print("version2 error:", error2)
    #print("version3 error:", error3)

    if error1 > error2:
        print("version2 is more accurate!")
    else:
        print("version1 is more accurate!")

    plt.plot([i for i in range(1, len(x)+1)], x, label='real')
    plt.plot([i for i in range(1, len(x)+1)], y, label='fm1')
    plt.plot([i for i in range(1, len(x)+1)], z, label='fm2')
    #plt.plot([i for i in range(1, len(x)+1)], w, label='upgrade_fm2')


    plt.xlabel("Num of items")
    plt.ylabel("Num of discriminated items")
    plt.legend()
    plt.show()
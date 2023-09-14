from itertools import combinations
import matplotlib.pyplot as plt


def make_candidate(freq_itemsets, k):
    candidates = set()
    for itemset1 in freq_itemsets:
        for itemset2 in freq_itemsets:
            union = itemset1 | itemset2
            if len(union) == k:
                for item in union:
                    if union - {item} not in freq_itemsets:
                        break
                    else: # 전부 다 있다. 정렬된 리스트 활용이 더 효율적.
                        candidates.add(union)

    return candidates

def filter(candidates, k, s):
    itemsets_cnt_k = {}
    with open("hw3\\groceries.csv", "r") as f:
        for line in f:
            basket = line.strip().split(",")
            for comb in combinations(basket, k):
                comb = frozenset(comb)
                if comb in candidates:
                    if comb not in itemsets_cnt_k:
                        itemsets_cnt_k[comb] = 0
                    itemsets_cnt_k[comb] += 1

    freq_itemsets = set(itemset for itemset, cnt in itemsets_cnt_k.items() if cnt >= s)
             
    return freq_itemsets, itemsets_cnt_k

def association_rule(I, itemsets_cnt_all, s, confidence, N):
    rules = []
    num_pc = 0
    num_I = itemsets_cnt_all[I]

    for i in range(1, len(I)):
        for A in combinations(fi, i): # A는 튜플, I는 frozenset
            A = frozenset(A)
            num_A = itemsets_cnt_all[A]
            num_diff_IA = itemsets_cnt_all[I-A]

            conf = num_I / num_A         
            
            if conf >= confidence:
                lift = conf * N / num_diff_IA
                if lift > 1:
                    num_pc += 1
                rule = str(set(A)) + ' -> ' + str(set(I-A))
                rules.append(rule)

    return rules, num_pc

item_cnt = {}
num_baskets = 0
with open("hw3\\groceries.csv", "r") as f: # 1 pass
    for line in f: 
        num_baskets += 1
        basket = line.strip().split(",")

        for item in basket:
            item = frozenset([item])
            if item not in item_cnt:
                item_cnt[item] = 0
            item_cnt[item] += 1

i = 0
for s in range(50, 350 , 50):
    freq_itemsets = set(item for item, cnt in item_cnt.items() if cnt >= s) # L1
    freq_itemsets_all = freq_itemsets.copy()
    itemsets_cnt_all = item_cnt.copy()

    k = 2
    while len(freq_itemsets) > 0:
        candidates = make_candidate(freq_itemsets, k) # C2
        freq_itemsets, itemsets_cnt_k = filter(candidates, k, s) # 2 pass, L2
        freq_itemsets_all |= freq_itemsets
        itemsets_cnt_all.update(itemsets_cnt_k)
        k += 1

    c = 0.05
    c_list = []
    total_num_pc_list = []
    while c < 1:
        c_list.append(c)
        all_rules = []
        total_num_pc = 0
        for fi in freq_itemsets_all:
            rules, num_pc =  association_rule(fi, itemsets_cnt_all, s, c, num_baskets)
            all_rules += rules
            total_num_pc += num_pc
        if len(all_rules) == 0:
            total_num_pc_list.append(0)
        else:
            total_num_pc_list.append(total_num_pc/len(all_rules))
        # print(s, c, total_num_pc)
        print('support =', s, ',', 'confidence =', c, ',', "# of rules =", len(all_rules), ',', all_rules)
        c += 0.1
    plt.plot(c_list, total_num_pc_list, label="support = " + str(s))

    i += 1


plt.xlabel("confidence")
plt.ylabel("proportion of positively correlation ")
plt.legend()
plt.show()


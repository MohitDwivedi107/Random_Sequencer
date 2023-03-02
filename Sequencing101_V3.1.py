import pandas as pd
import time
import math


class Sequencer:

    def __init__(self, data, n_comb):
        self.data = data
        self.n_comb = n_comb
        self.mac_avl = {m: 0 for m in data.columns[1:]}
        self.least_max_runtime = 9999
        self.jobs = []
        self.mac = []
        self.job_time = []
        self.best_arrangement = pd.DataFrame(columns=['Job', 'Machines', 'Time'])
        self.best_comb = ()
        self.best_history = {}
        self.comb_pool = []
        # self.create_comb_pool()

    def create_comb_pool(self):
        ''' This function will generate n_comb permutations as passed by user the result will be updated in variable comb_pool user can see the combinations by calling this variable via instance of Sequencer '''
        self.data.set_index(self.data.columns[0], inplace=True)
        self.data.fillna(9999, inplace=True)
        while len(self.comb_pool) + 1 <= self.n_comb and len(self.comb_pool) < math.factorial(len(self.data.index)):
            seq = list(self.data.sample(frac=1).index)
            if seq not in self.comb_pool:
                self.comb_pool.append(seq)
        return self.comb_pool

    def get_comp_mac(self, job):
        '''This will return all compatible machines for the job. Called from inner_funct'''
        try:
            return [mc for mc in self.data.loc[job].index if self.data.loc[job][mc] < 9999]
        except TypeError as e:
            print(e, 'Looks like data has some non int values')

    def find_best_mac(self, job, comp_mac, mac, mac_avl):
        '''This will return unique machine that is the best suitable for the job passed to it. It will allocate the job based on minimum waiting + completion time of the job for the machine.Called from inner_func'''
        orig_time = dict(zip(comp_mac, list(map(lambda m: self.data.loc[job][m], comp_mac))))
        d = {i: orig_time.get(i, 0) + mac_avl.get(i, 0) for i in set(orig_time).intersection(mac_avl)}
        min_val = min(d.values())
        bst_mc = [i for i in d.keys() if d[i] == min_val]
        if len(bst_mc) > 1:
            freq = list(map(lambda x: mac.count(x), bst_mc))
            d = dict(zip(bst_mc, freq))
            min_val = min(d.values())
            bst_mc = [i for i in sorted(d.keys()) if d[i] == min_val]
            if len(bst_mc) > 1:
                sub_mac_avl = {k: mac_avl[k] for k in bst_mc}
                bst_mc = [i for i in bst_mc if mac_avl[i] == min(sub_mac_avl.values())]
                return bst_mc[0]
        return bst_mc[0]

    def routing(self, job, ideal_mac):
        '''To assign the job to its decided machine'''
        self.jobs.append(job)
        self.mac.append(ideal_mac)
        self.job_time.append(self.data.loc[job][ideal_mac])
        self.mac_avl[ideal_mac] = self.mac_avl[ideal_mac] + self.data.loc[job][ideal_mac]

    def inner_func(self, job):
        '''This function works on individual job and checks if the job can be assigned to its ideal machine. If not possible then on next best machine'''
        if job not in self.jobs:
            compatible_mac = self.get_comp_mac(job)
            try:
                if len(compatible_mac) > 0:
                    ideal_mac = self.find_best_mac(job, compatible_mac, self.mac, self.mac_avl)
                if min(self.mac_avl.values()) == 0:
                    if self.mac_avl[ideal_mac] == 0:
                        self.routing(job, ideal_mac)
                    else:
                        best_alt = self.find_best_mac(job, compatible_mac, self.mac, self.mac_avl)
                        self.routing(job, best_alt)
                else:
                    best_alt = self.find_best_mac(job, compatible_mac, self.mac,
                                                  self.mac_avl)  # above condition i.e., assigning the job to its ideal line is not possible as that one is occupied.Hence, finding the alternate line that turns out to be the same line(as best one) then we will consider that only as best alternat
                    self.routing(job, best_alt)
            except TypeError as e:
                print(e, 'Did not receive anything in compatible machine(s) list')

    def control_flow(self, comb_pool_ele):
        ''' This function is responsible for validating the best combination '''
        [self.inner_func(j) for j in comb_pool_ele]
        if max(self.mac_avl.values()) < self.least_max_runtime:
            self.least_max_runtime = max(self.mac_avl.values())
            self.best_history = self.mac_avl
            self.best_comb = tuple(comb_pool_ele)
            self.best_arrangement['Job'] = self.jobs
            self.best_arrangement['Machines'] = self.mac
            self.best_arrangement['Time'] = self.job_time

        self.mac_avl = {m: 0 for m in self.data.columns}
        self.jobs = []
        self.mac = []
        self.job_time = []

    def main_func(self, comb_pool):
        '''Execution will start from here. this will pass each combination from combinaition pool and will call subsequent method internally.'''
        if len(comb_pool) > 0:
            [self.control_flow(comb_pool_ele) for comb_pool_ele in comb_pool]
        return self.best_comb, self.best_history, self.best_arrangement, self.least_max_runtime


if __name__ == '__main__':
    df1 = pd.read_csv(
        r'C:\Users\MohitDwivedi\OneDrive - TheMathCompany Private Limited\Desktop\PIP\D.O.D\Project\temp_data.csv')
    n = 100000
    start = time.time()
    s = Sequencer(df1, n)
    all_seq = s.create_comb_pool()
    bc, bh, ba, lrt = s.main_func(all_seq)
    end = time.time()
    print('\nTotal time taken to chk ', len(s.comb_pool), 'permutations', end - start, 'seconds')
    # print(bc)
    print(bh)
    print(ba)
    print(lrt)


import pandas as pd
import pylab as plt
import seaborn as sns

sample_char_file = '/scr/adenauer1/Franz/nki_sca/20150925_leicanki_sample.pkl'



######################
# GET SUBJECTS_LIST AND AGE
######################
df = pd.read_pickle(sample_char_file)


age_all = df.AGE_04.values
df = df[df.no_axis_1]
age_noaxis1 = df.AGE_04.values

sns.distplot(age_all, bins=20, color='b', label='all', kde=False)
sns.distplot(age_noaxis1, bins=20, color='g', label='no axis1', kde=False)
plt.legend()

plt.savefig('age_hist.png')

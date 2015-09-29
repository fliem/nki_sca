from sca import create_sca_wf

import os
from nipype.pipeline.engine import Node, Workflow, MapNode, JoinNode
import nipype.interfaces.utility as util
import nipype.interfaces.fsl as fsl
import nipype.interfaces.afni as afni
import nipype.interfaces.io as nio
from nipype import config
import pandas as pd
from utils import create_design_files, run_randomise_fct, create_renders_fct

wd_dir = '/scr/adenauer1/Franz/nki_sca/wd_stats'
ds_dir = '/scr/adenauer1/Franz/nki_sca/results'
in_data_base_dir = '/scr/adenauer1/Franz/nki_sca/results'
brain_img = '/scr/adenauer1/Franz/nki_sca/MNI152_T1_3mm_brain.nii.gz'
brain_mask_img = '/scr/adenauer1/Franz/nki_sca/MNI152_T1_3mm_brain_mask.nii.gz'
sample_char_file = '/scr/adenauer1/Franz/nki_sca/20150925_leicanki_sample.pkl'
out_subject_list = '/scr/adenauer1/Franz/nki_sca/subjects_used.csv'
path_str = os.path.join(in_data_base_dir, 'seed_based_z/_roi_{roi}/_subject_id_{subject_id}/corr_map_calc.nii.gz')

rois_list = [(-30, 9, -30),
             (30, 9, -30),
             (-30, -18, -21),
             (30, -18, -21),
             ]

rois_str_list = [str(r[0]) + '.' + str(r[1]) + '.' + str(r[2]) for r in rois_list]

######################
# GET SUBJECTS INFO
######################
df = pd.read_pickle(sample_char_file)



######################
# WF
######################
wf = Workflow(name='nki_sca_stats')
wf.base_dir = os.path.join(wd_dir)
nipype_cfg = dict(logging=dict(workflow_level='DEBUG'), execution={'stop_on_first_crash': True,
                                                                   'remove_unnecessary_outputs': True,
                                                                   'job_finished_timeout': 120})
config.update_config(nipype_cfg)
wf.config['execution']['crashdump_dir'] = os.path.join(wd_dir, 'crash')

ds = Node(nio.DataSink(base_directory=ds_dir), name='ds')



######################
# GET DATA
######################
age_infosource = Node(util.IdentityInterface(fields=['age_switch']), name='age_infosource')
age_infosource.iterables = ('age_switch', ['full_range', 'adults_only'])


def get_subjects_list_and_age_fct(age_switch, df):
    import os

    df = df[df.no_axis_1]

    if age_switch == 'adults_only':
        df = df[df.AGE_04 >= 18]

    subjects_list = df.leica_id.values
    age = df.AGE_04.values

    df_used = os.path.join(os.getcwd(), 'df_used.csv')
    df.to_csv(df_used)
    return (subjects_list, age, df_used)


get_subjects_list_and_age = Node(util.Function(input_names=['age_switch', 'df'], output_names=['subjects_list', 'age', 'df_used'],
                                               function=get_subjects_list_and_age_fct),
                                 name='get_subjects_list_and_age')
wf.connect(age_infosource, 'age_switch', get_subjects_list_and_age, 'age_switch')
get_subjects_list_and_age.inputs.df = df
wf.connect(get_subjects_list_and_age, 'df_used', ds, 'group.subjects_list')


roi_infosource = Node(util.IdentityInterface(fields=['roi']), name='roi_infosource')
roi_infosource.iterables = ('roi', rois_str_list)

# ######################
# # MERGE
# ######################
# CREATE LIST OF FILES FOR MERGE
def create_merge_list_fct(path_str, subjects_list, roi):
    out_files = []
    for s in subjects_list:
        out_files.append(path_str.format(roi=roi, subject_id=s))
    return out_files


create_merge_list = Node(util.Function(input_names=['path_str', 'subjects_list', 'roi'], output_names=['out_files'],
                                       function=create_merge_list_fct),
                         name='create_merge_list')
create_merge_list.inputs.path_str = path_str
wf.connect(roi_infosource, 'roi', create_merge_list, 'roi')
wf.connect(get_subjects_list_and_age, 'subjects_list', create_merge_list, 'subjects_list')


merge = Node(fsl.Merge(dimension='t'),
             name='merge')
wf.connect(create_merge_list, 'out_files', merge, 'in_files')
wf.connect(merge, 'merged_file', ds, 'group.merged')

mean_image = Node(fsl.MeanImage(), name='mean_image')
wf.connect(merge, 'merged_file', mean_image, 'in_file')
wf.connect(mean_image, 'out_file', ds, 'group.mean_group_image')



######################
# STATS
######################
# CREATE DESIGN FILES
design_files = Node(util.Function(input_names=['age'], output_names=['con_file', 'mat_file'],
                                  function=create_design_files),
                    name='design_files')
wf.connect(get_subjects_list_and_age, 'age', design_files, 'age')
wf.connect(design_files, 'con_file', ds, 'group.design.@con')
wf.connect(design_files, 'mat_file', ds, 'group.design.@mat')


# RUN RANDOMIZE
run_randomise = Node(util.Function(input_names=['data_file', 'mat_file', 'con_file', 'mask_file'],
                                   output_names=['out_dir'],
                                   function=run_randomise_fct),
                     name='run_randomise')
wf.connect(merge, 'merged_file', run_randomise, 'data_file')
wf.connect(design_files, 'con_file', run_randomise, 'con_file')
wf.connect(design_files, 'mat_file', run_randomise, 'mat_file')
run_randomise.inputs.mask_file = brain_mask_img

wf.connect(run_randomise, 'out_dir', ds, 'group.randomize')



# CREATE RENDERS
create_renders = Node(util.Function(input_names=['randomise_dir', 'n_cons', 'background_img'],
                                    output_names=['out_files_list'],
                                    function=create_renders_fct),
                      name='create_renders')
wf.connect(run_randomise, 'out_dir', create_renders, 'randomise_dir')
create_renders.inputs.background_img = brain_img
create_renders.inputs.n_cons = 2
wf.connect(create_renders, 'out_files_list', ds, 'group.randomize_renders')

######################
# RUN
######################

#
import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    wf.run(plugin='MultiProc', plugin_args={'n_procs': 31})
    # wf.run(plugin='CondorDAGMan')
wf.write_graph(dotfilename='sca', graph2use='exec', format='pdf')

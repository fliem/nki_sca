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

wd_dir = '/scr/adenauer1/Franz/nki_sca/wd'
ds_dir = '/scr/adenauer1/Franz/nki_sca/results'
in_data_base_dir = '/scr/adenauer2/Franz/LeiCA_NKI/results'
brain_img = '/scr/adenauer1/Franz/nki_sca/MNI152_T1_3mm_brain.nii.gz'
brain_mask_img = '/scr/adenauer1/Franz/nki_sca/MNI152_T1_3mm_brain_mask.nii.gz'
sample_char_file = '/scr/adenauer1/Franz/nki_sca/20150925_leicanki_sample.pkl'
out_subject_list = '/scr/adenauer1/Franz/nki_sca/subjects_used.csv'

rois_list = [(-30, 9, -30),
             (30, 9, -30),
             (-30, -18, -21),
             (30, -18, -21),
             ]



######################
# GET SUBJECTS_LIST
######################
df = pd.read_pickle(sample_char_file)
df = df[df.no_axis_1]
df.to_csv(out_subject_list)
#########

subjects_list = df.leica_id.values


######################
# WF
######################
wf = Workflow(name='nki_sca_pipeline')
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
# SUBJECTS ITERATOR
subjects_infosource = Node(util.IdentityInterface(fields=['subject_id']), name='subjects_infosource')
subjects_infosource.iterables = ('subject_id', subjects_list)

roi_infosource = Node(util.IdentityInterface(fields=['roi']), name='roi_infosource')
roi_infosource.iterables = ('roi', rois_list)


# GET SUBJECT SPECIFIC FUNCTIONAL DATA
selectfiles_templates = {
    'preproc_epi_mni': '{subject_id}/rsfMRI_preprocessing/epis_MNI_3mm/03_denoised_BP_tNorm/TR_645/residual_filt_norm_warp.nii.gz',
}

selectfiles = Node(nio.SelectFiles(selectfiles_templates,
                                   base_directory=in_data_base_dir),
                   name="selectfiles")
wf.connect(subjects_infosource, 'subject_id', selectfiles, 'subject_id')


######################
# SCA
######################
# FOR EACH SUBJECT CALCULATE SCA MAP
sca_wf = create_sca_wf(working_dir=wd_dir, name='sca_single_subject')
wf.connect(selectfiles, 'preproc_epi_mni', sca_wf, 'inputnode.rs_preprocessed')
sca_wf.inputs.inputnode.MNI_template = brain_img
wf.connect(roi_infosource, 'roi', sca_wf, 'inputnode.roi_coords')

wf.connect(sca_wf, 'outputnode.seed_based_z', ds, 'seed_based_z')
wf.connect(sca_wf, 'outputnode.roi_img', ds, 'roi')


######################
# RUN
######################
wf.write_graph(dotfilename='sca', graph2use='exec', format='pdf')

#
import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    wf.run(plugin='MultiProc', plugin_args={'n_procs': 32})
    #wf.run(plugin='CondorDAGMan')

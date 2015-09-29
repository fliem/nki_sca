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

rois_list = [(-30, 9, -30),
             (30, 9, -30),
             (-30, -18, -21),
             (30, -18, -21),
             ]



######################
# GET SUBJECTS_LIST AND AGE
######################
df = pd.read_pickle(sample_char_file)
df = df.iloc[:3]  # ['0166987', '0105290', '0192736'],
age = df.AGE_04.values
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


# CREATE LIST OF FILES FOR MERGE
def create_list_fct(in_files):
    print in_files
    out_files = in_files
    return out_files


create_list = JoinNode(util.Function(input_names=['in_files'], output_names=['out_files'],
                                     function=create_list_fct),
                       name='create_list',
                       joinsource='subjects_infosource',
                       joinfield=['in_files'])
wf.connect(sca_wf, 'outputnode.seed_based_z', create_list, 'in_files')

merge = Node(fsl.Merge(dimension='t'),
             name='merge')
wf.connect(create_list, 'out_files', merge, 'in_files')
wf.connect(merge, 'merged_file', ds, 'merged')

mean_image = Node(fsl.MeanImage(), name='mean_image')
wf.connect(merge, 'merged_file', mean_image, 'in_file')
wf.connect(mean_image, 'out_file', ds, 'mean_group_image')


######################
# STATS
######################
# CREATE DESIGN FILES
design_files = Node(util.Function(input_names=['age'], output_names=['con_file', 'mat_file'],
                                  function=create_design_files),
                    name='design_files')
design_files.inputs.age = age
wf.connect(design_files, 'con_file', ds, 'design.@con')
wf.connect(design_files, 'mat_file', ds, 'design.@mat')


# RUN RANDOMIZE
run_randomise = Node(util.Function(input_names=['data_file', 'mat_file', 'con_file', 'mask_file'],
                                   output_names=['out_dir'],
                                   function=run_randomise_fct),
                     name='run_randomise')
wf.connect(merge, 'merged_file', run_randomise, 'data_file')
wf.connect(design_files, 'con_file', run_randomise, 'con_file')
wf.connect(design_files, 'mat_file', run_randomise, 'mat_file')
run_randomise.inputs.mask_file = brain_mask_img

wf.connect(run_randomise, 'out_dir', ds, 'randomize')



# CREATE RENDERS
create_renders = Node(util.Function(input_names=['randomise_dir', 'n_cons', 'background_img'],
                                    output_names=['out_files_list'],
                                    function=create_renders_fct),
                      name='create_renders')
wf.connect(run_randomise, 'out_dir', create_renders, 'randomise_dir')
create_renders.inputs.background_img = brain_img
create_renders.inputs.n_cons = 2
wf.connect(create_renders, 'out_files_list', ds, 'randomize_renders')

######################
# RUN
######################
wf.write_graph(dotfilename='sca', graph2use='exec', format='pdf')

#
import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    # wf.run(plugin='MultiProc', plugin_args={'n_procs': 4})
    wf.run(plugin='CondorDAGMan')

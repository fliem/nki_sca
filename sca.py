'''
Lifted from mindwandering.calculate_measures
'''
import os
from nipype.pipeline.engine import Node, Workflow, MapNode
import nipype.interfaces.utility as util
import nipype.interfaces.fsl as fsl
import nipype.interfaces.afni as afni
import nipype.interfaces.io as nio




def create_sca_wf(working_dir, name='sca'):
    afni.base.AFNICommand.set_default_output_type('NIFTI_GZ')
    fsl.FSLCommand.set_default_output_type('NIFTI_GZ')

    sca_wf = Workflow(name=name)
    sca_wf.base_dir = os.path.join(working_dir)

    # inputnode
    inputnode = Node(util.IdentityInterface(fields=['rs_preprocessed', 'MNI_template', 'roi_coords']),
                     name='inputnode')

    # outputnode
    outputnode = Node(util.IdentityInterface(fields=['functional_mask',
                                                     'seed_based_z',
                                                     'roi_img']),
                      name='outputnode')




    epi_mask = Node(interface=afni.Automask(), name='epi_mask')
    sca_wf.connect(inputnode, 'rs_preprocessed', epi_mask, 'in_file')
    sca_wf.connect(epi_mask, 'out_file', outputnode, 'functional_mask')

    def roi2exp_fct(coord):
        return 'step(4-(x%+d)*(x%+d)-(y%+d)*(y%+d)-(z%+d)*(z%+d))'%(coord[0], coord[0], coord[1], coord[1], -coord[2], -coord[2])

    roi2exp = Node(util.Function(input_names=['coord'],
                                       output_names=['expr'],
                                       function=roi2exp_fct),
                         name='roi2exp')
    sca_wf.connect(inputnode, 'roi_coords', roi2exp, 'coord')


    point = Node(afni.Calc(), name='point')
    sca_wf.connect(inputnode, 'MNI_template', point, 'in_file_a')
    point.inputs.outputtype = 'NIFTI_GZ'
    point.inputs.out_file = 'roi_point.nii.gz'
    sca_wf.connect(roi2exp, 'expr', point, 'expr')

    def format_filename(roi_str):
        import string
        valid_chars = '-_.%s%s' % (string.ascii_letters, string.digits)
        return 'roi_'+''.join(c for c in str(roi_str).replace(',','_') if c in valid_chars)+'_roi.nii.gz'

    sphere = Node(fsl.ImageMaths(), name='sphere')
    sphere.inputs.out_data_type = 'float'
    sphere.inputs.op_string = '-kernel sphere 4 -fmean -bin'
    sca_wf.connect(point, 'out_file', sphere, 'in_file')
    sca_wf.connect(inputnode, ('roi_coords', format_filename), sphere, 'out_file')
    sca_wf.connect(sphere, 'out_file', outputnode, 'roi_img')

    extract_timeseries = Node(afni.Maskave(), name='extract_timeseries')
    extract_timeseries.inputs.quiet = True
    sca_wf.connect(sphere, 'out_file', extract_timeseries, 'mask')
    sca_wf.connect(inputnode, 'rs_preprocessed', extract_timeseries, 'in_file')

    correlation_map = Node(afni.Fim(), name='correlation_map')
    correlation_map.inputs.out = 'Correlation'
    correlation_map.inputs.outputtype = 'NIFTI_GZ'
    correlation_map.inputs.out_file = 'corr_map.nii.gz'
    sca_wf.connect(extract_timeseries, 'out_file', correlation_map, 'ideal_file')
    sca_wf.connect(inputnode, 'rs_preprocessed', correlation_map, 'in_file')

    z_trans = Node(interface=afni.Calc(), name='z_trans')
    z_trans.inputs.expr = 'log((1+a)/(1-a))/2'
    z_trans.inputs.outputtype = 'NIFTI_GZ'
    sca_wf.connect(correlation_map, 'out_file', z_trans, 'in_file_a')
    sca_wf.connect(z_trans, 'out_file', outputnode, 'seed_based_z')


    sca_wf.write_graph(dotfilename='sca', graph2use='flat', format='pdf')


    return sca_wf


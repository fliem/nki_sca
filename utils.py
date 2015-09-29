def create_design_files(age):
    import os

    mat = age

    mat_str = [
        '/NumWaves 1',
        '/NumPoints %s' % str(mat.shape[0]),
        '/Matrix'
    ]

    n_cons = 2
    cons_str = [
        '/ContrastName1 pos_age',
        '/ContrastName2 neg_age',
        '/NumWaves 1',
        '/NumContrasts %s' % str(n_cons),
        '',
        '/Matrix',
        '1',
        '-1',
    ]
    mat_file = os.path.join(os.getcwd(), 'design.mat')
    con_file = os.path.join(os.getcwd(), 'design.con')

    with open(con_file, 'w') as out_file:
        for line in cons_str:
            out_file.write('%s\n' % line)

    with open(mat_file, 'w') as out_file:
        for line in mat_str:
            out_file.write('%s\n' % line)
        for line in mat:
            out_file.write('%s\n' % line)

    return (con_file, mat_file)


def run_randomise_fct(data_file, mat_file, con_file, mask_file):
    import os
    out_dir = os.path.join(os.getcwd(), 'glm')
    os.mkdir(out_dir)
    cmd_str = 'randomise -i %s -o glm/glm -d %s -t %s -m %s -n 500 -D -T' % (data_file, mat_file, con_file, mask_file)
    file = open('command.txt', 'w')
    file.write(cmd_str)
    file.close()
    os.system(cmd_str)

    return out_dir


def create_renders_fct(randomise_dir, n_cons, background_img):
    import os
    thresh = .95
    out_files_list = []
    corr_list = ['glm_tfce_corrp_tstat']
    for corr in corr_list:
        for con in range(1, n_cons + 1):
            output_root = corr + str(con)
            in_file = os.path.join(randomise_dir, output_root + '.nii.gz')
            out_file = os.path.join(os.getcwd(), 'rendered_' + output_root + '.png')
            out_files_list.append(out_file)
            cmd_str = 'easythresh %s %s %s %s' % (in_file, str(thresh), background_img, output_root)

            file = open('command.txt', 'w')
            file.write(cmd_str)
            file.close()

            os.system(cmd_str)
    return out_files_list

import os, time

# script to create tar-ball for grid submission scripts
# usage: python3 tarball_create_script.py

tarball_name = 'MyToolAnalysis_grid.tar.gz'
folder_path = '/exp/annie/app/users/<user>/'
folder_name = 'EventBuilding/'

tar_command = 'tar -czvf ' + tarball_name + ' -C ' + folder_path + ' ' + folder_name

print('\nTar-ing folder (details below)')
print(' - tar-ball name: ' + tarball_name)
print(' - folder path:   ' + folder_path)
print(' - folder name:   ' + folder_name)
print('\n')
print('Full command: ' + tar_command)
print('\n')

time.sleep(3)

os.system(tar_command)

print('\ndone')

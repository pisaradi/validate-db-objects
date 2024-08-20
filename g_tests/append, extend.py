# import pandas

# src_dic = {'txt_key': [1, 2]}
# src_list = ['a', 'b']

# add_list1 = ['c']
# add_list2 = [['d', 'e']]

# # APPEND - adds the whole adding element to the source list
# #   (doesn't matter if it's the part of source dictionary or source list) 
# src_dic['txt_key'].append(add_list1)
# src_list.append(add_list1)
# print('output APPEND1', '\n', src_dic, '\n', src_list)
# src_dic['txt_key'].append(add_list2)
# src_list.append(add_list2)
# print('output APPEND2', '\n', src_dic, '\n', src_list)

# # EXTEND - adds the 1st level content of a variable
# #   (doesn't matter if it's the part of source dictionary or source list)
# #   by the elements inside the most outer list
#   # reset variables
# src_dic = {'txt_key': [1, 2]}
# src_list = ['a', 'b']

# add_list1 = ['c']
# add_list2 = [['d', 'e']]

# src_dic['txt_key'].extend(add_list1)
# src_list.extend(add_list1)
# print('output EXTEND1', '\n', src_dic, '\n', src_list)
# src_dic['txt_key'].extend(add_list2)        
# src_list.extend(add_list2)
# print('output EXTEND2', '\n', src_dic, '\n', src_list)

# # new list 1 - comma in [] adds the whole adding element to the source list
#   # reset variables
# src_dic = {'txt_key': [1, 2]}
# src_list = ['a', 'b']

# add_list1 = ['c']
# add_list2 = [['d', 'e']]

# new_list_dic = [src_dic['txt_key'], add_list1]
# print('output new list 1', '\n', new_list_dic)
# print(pandas.DataFrame(new_list_dic))

# new_list_dic = [src_dic['txt_key'], add_list2]
# print('output new list 1', '\n', new_list_dic)
# print(pandas.DataFrame(new_list_dic))


# # new list 2 - sign plus adds the 1st level content of a variable (similarly as extend)
#   # reset variables
# src_dic = {'txt_key': [1, 2]}
# src_list = ['a', 'b']

# add_list1 = ['c']
# add_list2 = [['d', 'e']]

# new_list_dic = src_dic['txt_key'] + add_list1     # new_list_dic = [src_dic['txt_key'] + add_list1]
# print('output new list 2', '\n', new_list_dic)
# print(pandas.DataFrame(new_list_dic))

# new_list_dic = src_dic['txt_key'] + add_list2     # new_list_dic = [src_dic['txt_key'] + add_list1]
# print('output new list 2', '\n', new_list_dic)
# print(pandas.DataFrame(new_list_dic))


# add a list to a dictionary key that contains list
dict1_1 = {'1_1': [1, 2]}
list1_2 = ['a', 'b']
dict1_1['1_1'].extend(list1_2)
print(dict1_1)

list1_2.append('a')
print(list1_2)


dict2_1 = '1'
list2_2 = ['a', 'b']
print( [dict2_1] + list2_2 )



#!/usr/bin/env python
# _*_ coding:utf-8 _*_

u"""this script is used to create distribute tasks on remote host"""

import sys
import requests
import os
import re
from json import dump, load, loads
from getopt import getopt, GetoptError


def trans_file_to_list(filename):
    """open host-file and transfer file per line to a list"""
    try:
        file_ = open(filename, 'rb')
        list_ = [line[:-1] for line in file_]
        list_ = [str(i).strip() for i in list_]
    except IOError, err:
        print "打开文件失败" + str(err)
    finally:
        file_.close()

    return list_


def check_validation(
        data_module, task_string, distri_file,
        host_list, input_file, module_tasks_iter,
        task_list,flag_dep):
    """
    首先有了inputfile
    1、-t指定的要分布的task必须是inputfile中已经有了的taskname
    2、-i指定inputfile之后，对其合理性进行检查
    3、-d指定了要分布的计算机的主机名，然后判断，它的主机名是否出现在
    4、host.txt中不能出现重复的主机名
    4、host.txt中的主机名必须在dagobah上已经存在
    6、判断有没有name, command, hostname, 使用dict.has_key()来判断
    """

    # -t和hostlist的重复性测试
    check_repeat(u"-t 指定的值： " + task_string, task_list)
    check_repeat(distri_file, host_list)

    #---------检查job必备键,及其job键列表的重复性-----------------
    module_key_list = data_module.keys()
    list_ = [u"name",u"tasks",u"cron_schedule",u"dependencies",u"notes"]

    if not check_issub(module_key_list,list_):
        print "模板必备字段: ", list_, " ,其中'dependencies'字段可选"
        if flag_dep == 0:
            module_key_list.remove(u'dependencies')
        print "你的字段列表： ", module_key_list
        s1 = set(module_key_list).difference(set(list_))
        print "Error: 未知字段： ",
        for i in list(s1):
            print i,
        sys.exit()
    elif len(module_key_list) < 5:
        print "模板必备字段: ", list_, " ,其中'dependencies'字段可选"
        if flag_dep == 0:
            module_key_list.remove(u'dependencies')
            list_.remove(u'dependencies')
        print "你的字段列表: ", module_key_list
        s1 = set(list_).difference(set(module_key_list))
        print "Error: 缺少字段: ",
        for i in list(s1):
            print u"\""+i+u"\"  ",
        sys.exit()





    #四件不为空检查

    check_if_null(data_module,'name')
    check_if_null(data_module,'cron_schedule')
    check_if_null(data_module,'notes')
    check_if_null(data_module,'tasks')


    # check cron_syntax
    check_schedule_syntax(data_module)

    for i in module_tasks_iter:
        if 'name' not in data_module['tasks'][i].keys():
            print data_module['tasks'][i]
            print "Error: 该任务缺少name字段!"
            sys.exit(1)
        if 'command' not in data_module['tasks'][i].keys():
            print data_module['tasks'][i]
            print "Error: 该任务缺少command字段!"
            sys.exit(1)


    module_task_list = [data_module[u'tasks'][i][u'name'] for i in module_tasks_iter]







    # -------------检查tasks内部----------
    #check_repeat(input_file + u'中tasks中的name', module_task_list)


    for id_ in module_tasks_iter:
        if 'name' not in data_module['tasks'][id_].keys():
            print data_module['tasks'][id_]
            print "Error: 这个任务中任务缺少name字段，请重新检查!"
            sys.exit(1)
        else:
            name = data_module['tasks'][id_]['name']
            if name == "" or name == None:
                print data_module['tasks'][id_]
                print "Error: 这个任务中的name值不能为空！"
                sys.exit(1)

        if 'command' not in data_module['tasks'][id_].keys():
            print data_module['tasks'][id_]
            print "Error: 这个任务缺少command字段，请重新检查!", input_file
            sys.exit(1)
        else:
            command = data_module['tasks'][id_]['command']
            if command == "" or command == None:
                print data_module['tasks'][id_]
                print "Error: 这个任务中的command值不能为空！"
                sys.exit(1)


    # 将要修改的task和模板中的task_name做比较，如果要修改的task不是模板中的子集就报错
    if not check_issub(task_list, module_task_list):
        print u"模板中task_name列表: " + str(module_task_list)
        print u"-t指定的name列表: " + str(task_list)
        s1 = set(task_list).difference(set(module_task_list))
        print u"Error: 指定列表中有未知name: ",
        for i in list(s1):
            print "\""+i+"\".",
        sys.exit(1)


    for id_ in module_tasks_iter:
        command = data_module['tasks'][id_]['command']
        name = data_module['tasks'][id_]['name']

        if u"soft_timeout" in data_module['tasks'][id_].keys():
            soft_timeout = data_module['tasks'][id_]['soft_timeout']

            if soft_timeout == "" or soft_timeout == None:
                print data_module['tasks'][i]
                print u"Error: soft_timeout不能为空!"
                sys.exit(1)

            if not str(soft_timeout).isdigit():
                print data_module['tasks'][i]
                print u"Error: soft_timeout必须是整数!"
                sys.exit(1)


        if u"hard_timeout" in data_module['tasks'][id_].keys():
            hard_timeout = data_module['tasks'][id_]['hard_timeout']

            if hard_timeout == "" or hard_timeout == None:
                print data_module['tasks'][i]
                print u"Error: hard_timeout不能为空"
                sys.exit(1)

            if not str(hard_timeout).isdigit():
                print u"Error: hard_timeout必须是整数!"
                sys.exit(1)

        if name not in task_list:
            if u"hostname" not in data_module['tasks'][id_].keys():
                print data_module['tasks'][i]
                print u"Error: 不做分布式的任务必须具备hostname字段，请重新编辑"
                sys.exit(1)
            else:
                hostname = data_module['tasks'][id_]['hostname']
                if hostname == "" or hostname == None:
                    print data_module['tasks'][i]
                    print u"Error: 不做分布式的任务hostname值不能为空"
                    sys.exit(1)
                else:
                    check_if_in_serv_host()
                    # 将来做服务器检查
                    pass


    if "dependencies" in module_key_list:
        module_depend_key_list = data_module[u'dependencies'].keys()

        print module_depend_key_list
        check_repeat(input_file + u'中dependencies中的name', module_depend_key_list)

        if not check_issub(module_depend_key_list, module_task_list):
            print "模板中task_name列表：" + str(module_task_list)
            print "依赖性键名列表：" + str(module_depend_key_list)
            print "依赖性键名列表中某个键不在已知的taskname列表中，请重新检查inputfile的depend字段"
            sys.exit(1)

        for i in module_depend_key_list:
            check_repeat(u"dependencies字段有问题: \"" + i + u"\": " + str(data_module['dependencies'][i]), data_module['dependencies'][i])
            if not check_issub(data_module['dependencies'][i], module_task_list):
                print "模板中task_name列表：" + str(module_task_list)
                print "问题点：" + str(data_module['dependencies'][i])
                print "依赖性键的值列表中某个元素不在已知的taskname列表中，请重新检查inputfile的depend字段"
                sys.exit(1)


def check_if_in_serv_host():
    '''检查不再分布式任务的hostname值是否在serverhost列表中'''

    pass


def check_if_null(data,arg):
    if data[arg] == "" or data[arg] == None or data[arg] == []:
        print "Error: ",arg, "字段值不能空"
        sys.exit(1)


def check_schedule_syntax(data_module):
    """check if the item is matching cron syntax"""
    string_ = data_module['cron_schedule']
    list_each_char = ' '.join(string_).split()
    list_schedule = data_module['cron_schedule'].split()

    for i in list_each_char:
        if i not in ['*',',','/','-','0','1','2','3','4','5','6','7','8','9']:
            print "\"cron_schedule\": \"", string_, "\""
            print "Error: cron_schedule非法字符：", i
            sys.exit(1)

    if not len(list_schedule) == 5:
        print "\"cron_schedule\": \"", string_, "\""
        print "Error: schedule的格式有误，至少有5个字段"
        sys.exit(1)


def check_input_file(data_module):
    """检查inputfile中的所有的字段是否合法：
    1、name不能为空
    2、schedule可以为空
    3、必须有name和dependencies,notes,tasks,cron_schedule
    4.name不能含有中文字符。name不能为null，或者""，name和job上面的任务冲突就报警。
    5、notes也不能含有中文字符
    3、tasks的每个字段至少包含hostname，name，command，soft和hard可选
    7、dep可以有也可以没有，没有代表无，
    8、cron_schedule可以有也可以无，但是不能出错。
    9、hostname要在服务端存在。
    10,没有在-t指定的task必须要有hostname，并且hostname必须存在于服务器中
    """
    module_key_list = data_module.keys()
    # 检查模块键列表的重复性
    check_repeat(str(module_key_list),module_key_list)
    list_ = ["name","tasks","cron_schedule","dependencies","notes"]
    list_2 = ["name","tasks","cron_schedule","notes"]
    # 检查模块中键的合法性：dependencies可选，其他的都要一样
    check_issub(module_key_list,list_)
    if len(module_key_list) != len(list_):
        if not check_issub(module_key_list,list_):
            print "模块的'name','cron_schedule','notes','tasks'键必须有并且不能重复，'dependencies'可选"
            sys.exit(1)
    elif set(module_key_list) != set(list_2):
            print "模块的'name','cron_schedule','notes','tasks'键必须有并且不能重复，'dependencies'可选"
            sys.exit(1)
    else:
        pass

    if "dependencies" in module_key_list:
        module_depend_key_list


def integration(module_tasks_iter,data_module):
    """将tasks中task的id和其name对应的值整合成一个dict"""
    dict_ = {}
    for i in module_tasks_iter:
        dict_[i] = data_module['tasks'][i]['name']

    return dict_


def check_repeat(describe, list_):
    """check if the list_ has repeated item"""

    if len(list_) != len(set(list_)):
        print u"Error: " + describe + u" 有重复字段"
        print str(list_)
        sys.exit(1)

def check_tasks_name_repeat(data_module,module_tasks_iter):
    dict_id_name = integration(module_tasks_iter,data_module)
    values = dict_id_name.values()
    for id_1 in dict_id_name:
        yuanben = dict_id_name[id_1]
        values.remove(yuanben)
        if yuanben in values:
            print data_module['tasks'][id_1]
            del dict_id_name[id_1]
            for id_2 in dict_id_name:
                if dict_id_name[id_2] == yuanben:
                    print data_module['tasks'][id_2]
                    print "Error: 这两个任务的name重复，请更正!"
                    sys.exit(1)
        else:
            pass


def check_issub(list1, list2):
    """return true if list1 is the subset of list2"""

    set1 = set(list1)
    set2 = set(list2)

    return set1.issubset(set2)


def decode_import_json(json_doc, transformers=None):
    """ Decode a JSON string based on a list of transformers.
    Each transformer is a pair of ([conditional], transformer). If
    all conditionals are met on each non-list, non-dict object,
        the transformer tries to apply itself.
        conditional: Callable that returns a Bool.
        transformer: Callable transformer on non-dict, non-list objects.
        """

    def custom_decoder(dct):

        def transform(o):

            if not transformers:
                return o

            for conditionals, transformer in transformers:

                conditions_met = True
                for conditional in conditionals:
                    try:
                        condition_met = conditional(o)
                    except:
                        condition_met = False
                    if not condition_met:
                        conditions_met = False
                        break

                if not conditions_met:
                    continue

                try:
                    return transformer(o)
                except:
                    pass

            return o

        for key in dct.iterkeys():
            if isinstance(key, dict):
                custom_decoder(dct[key])
            elif isinstance(key, list):
                [custom_decoder[elem] for elem in dct[key]]
            else:
                dct[key] = transform(dct[key])

        return dct

    return loads(json_doc, object_hook=custom_decoder)


def check_list_be_same(list_1, list_2):
    """这个函数目前暂时作废"""

    list_1.sort()
    list_2.sort()

    if list_1 != list_2:
        list_1.remove(u'dependencies')
        list_2.remove(u'dependencies')
        print "Error: job必备字段： ",
        print list_2,
        print "'dependencies' 字段可选"
        print "你的字段列表： ",
        print list_1
        #print u"你的字段列表：" + str(list_1.remove('dependencies'))
        print u"请纠正相应字段."
        sys.exit(1)


def get_unchanged_task_id(module_tasks_iter, data_module, task_list):
    """获取没有在改变列表的task在tasks中的index列表"""
    list_ = []

    for i in module_tasks_iter:
        if data_module[u'tasks'][i][u'name'] not in task_list:
            list_.append(data_module[u'tasks'].index(data_module[u'tasks'][i]))

    return list_


def get_id_by_name(data_module, module_tasks_iter, name):
    """get task id by it's name"""
    for i in module_tasks_iter:

        if data_module['tasks'][i]['name'] == name:

            return i


def init_data_real(data_module):
    """
    初始化一个只包含tasks, notes, name, cron_schedule, dependencies
    的一个字典，但是task和dependencies是空的，其他直接继承data_module
    """
    data_real = add_empty_job()

    data_real["notes"] = data_module['notes']

    data_real["name"] = data_module['name']

    data_real["cron_schedule"] = data_module['cron_schedule']

    return data_real


def update_data_real(data_module, data_real,
                     task_id, hostname,
                     task_list, module_tasks_iter):
    """"""
    dict_ = add_empty_task()

    if 'soft_timeout' in data_module['tasks'][task_id]:
        dict_['soft_timeout'] = data_module['tasks'][task_id]['soft_timeout']

    if 'hard_timeout' in data_module['tasks'][task_id]:
        dict_['hard_timeout'] = data_module['tasks'][task_id]['hard_timeout']

    dict_['command'] = data_module['tasks'][task_id]['command']

    old_name = data_module['tasks'][task_id]['name']

    dict_['name'] = old_name + u'_at_' + hostname

    dict_['hostname'] = hostname

    data_real['tasks'].append(dict_)

    # 不论线花到哪里，进入此函数的都是要被分布的task，也就是说，哪怕你把所有的任务都圈起来，这里面的东西也只有两种情况：在列表和不在列表！
    # 初始化的对应dependencies的字段都是一个空list，然后从data_module中的字段来判断选择，如果是属于-t指定的task之内的，就将其更名，并添加至新列表
    # 如果是之外的，就将原来的添加进去
    list_ = []
    if data_module['dependencies'][old_name] == []:
        data_real['dependencies'][dict_['name']] = list_
    else:

        for i in data_module['dependencies'][old_name]:

            if i in task_list:
                new = i + u'_at_' + hostname
                list_.append(new)
                data_real['dependencies'][dict_['name']] = [].append(new)
            else:
                i_id = get_id_by_name(data_module, module_tasks_iter, i)
                new = i + u'_at_' + data_module['tasks'][i_id]['hostname']
                list_.append(new)
                data_real['dependencies'][dict_['name']] = [].append(new)

        data_real['dependencies'][dict_['name']] = list_


def update_data_real2(data_module, data_real,
                      task_id, task_list,
                      host_iter, host_list,
                      module_tasks_iter):
    """update unchanegd tasks in module_task_list"""

    dict_ = add_empty_task()

    if 'soft_timeout' in data_module['tasks'][task_id]:
        dict_['soft_timeout'] = data_module['tasks'][task_id]['soft_timeout']

    if 'hard_timeout' in data_module['tasks'][task_id]:
        dict_['hard_timeout'] = data_module['tasks'][task_id]['hard_timeout']

    dict_['command'] = data_module['tasks'][task_id]['command']
    old_name = data_module['tasks'][task_id]['name']
    dict_['hostname'] = data_module['tasks'][task_id]['hostname']
    dict_['name'] = old_name + u'_at_' + dict_['hostname']
    data_real['tasks'].append(dict_)
    list_1 = []
    update_data_real3(data_module, data_real,
                      task_id, host_iter,
                      host_list, task_list,
                      list_1, dict_['name'],
                      module_tasks_iter)


# 不论线花到哪里，进入此函数的都是要被分布的task，也就是说，哪怕你把所有的任务都圈起来，这里面的东西也只有两种情况：在列表和不在列表！
# 初始化的对应dependencies的字段都是一个空list，然后从data_module中的字段来判断选择，如果是属于-t指定的task之内的，就将其更名，并添加至新列表
# 如果是之外的，就将原来的添加进去
def update_data_real3(data_module, data_real,
                      task_id, host_iter,
                      host_list, task_list,
                      list_1, name1,
                      module_tasks_iter):
    """update_unchanged_task_dependencies"""

    name = data_module['tasks'][task_id]['name']
    data_real['dependencies'][name1] = []
    for i in data_module['dependencies'][name]:
        if i in task_list:
            for host in host_iter:
                new = i + u'_at_' + host_list[host]
                list_1.append(new)
        else:
            i_id = get_id_by_name(data_module, module_tasks_iter, i)
            new = i + u'_at_' + data_module['tasks'][i_id]['hostname']
            list_1.append(new)
    data_real['dependencies'][name1] = list_1


def add_empty_task():
    '''add empty task to tasks, equal to init task unit'''

    dict_ = {}
    dict_[u"soft_timeout"] = 0
    dict_[u"hard_timeout"] = 0
    dict_[u"hostname"] = None
    dict_[u"name"] = None
    dict_[u"command"] = None

    return dict_


def add_empty_job():
    '''add empty jobs, equal to init job unit'''

    dict_ = {}
    dict_[u'notes'] = None
    dict_[u'name'] = None
    dict_[u'cron_schedule'] = None
    dict_[u'tasks'] = []
    dict_[u'dependencies'] = {}

    return dict_


def jsonalize(data_real):
    '''创建连接dagobah的回话，然后开始导入数据'''
    filename = 'jiushizheige.json'

    try:
        tmpfile = open(filename, 'wb')
        dump(data_real, tmpfile)
        tmpfile.flush()
    except IOError:
        print "打开文件失败: " + filename
        sys.exit(1)
    finally:
        tmpfile.close()

    try:
        file_ = open(filename, 'rb')

        session = requests.session()

        try:
            session.post('http://localhost:9000/do-login', {"password": "dagobah"})
        except requests.exceptions.ConnectionError, err:
            print str(err)
            print "发送终止"
            sys.exit(1)

        import_url = "http://localhost:9000/jobs/import"

        json_file = [('file', (filename, file_, 'application/json'))]

        session.post(import_url, files=json_file)

        print "导入成功"

    finally:
        session.close()
        file_.close()
        os.remove(filename)


def usage():
    print """python dagopost.py <options> <args>
        -i | --input-file = <jsonfile>
        -h | --host-file = <hostfile>
        -t | --task-to-distribute = <tasks>"""


def main():
    try:
        opts, args = getopt(
            sys.argv[1:], 'i:t:h:',
            [
                'input-file=',
                'task-to-distribute=',
                'host-file='
                ])
    except GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)

    for opt, value in opts:

        if opt in ("--input-file", "-i"):
            input_file = value
        elif opt in ("--task-to-distribute", "-t"):
            task_string = value
        elif opt in ("--host-file", "-h"):
            distri_file = value
        else:
            assert False, "unhandled option"

    try:
        inputfile = open(input_file, 'rb')
        try:
            #  data_module2 = load(inputfile)
            #  print data_module2
            data_module = decode_import_json(inputfile.read())
        except ValueError, err:
            print "\"" + input_file + "\"格式问题:"
            print err.__class__.__name__, err
            sys.exit(1)
        finally:
            inputfile.close()
    except IOError:
            print "文件打开错误"
            sys.exit(1)

    # 要修改(分布)的task的name列表, 这些列表从外部来，服从的是coding，要转换为unicode和原来数据比较
    task_list = filter(None, task_string.split(','))
    task_list = [unicode(x, 'utf-8') for x in task_list]

    #  获取要分布的主机名的列表
    host_list = trans_file_to_list(distri_file)
    #host_list = [unicode(x, 'utf-8') for x in host_list]
    host_iter = range(len(host_list))

    # 所有模板中的任务数
    module_tasks_iter = range(len(data_module[u'tasks']))


    # 用户的dep完全可以不写代表单个任务
    flag_dep = 1
    if u'dependencies' not in data_module:
        data_module[u'dependencies'] = {}
        flag_dep = 0

    # check validation
    check_validation(
        data_module, task_string, distri_file,
        host_list, input_file, module_tasks_iter,
        task_list,flag_dep)

    # 模板中tasks中的name列表
    module_task_list = [data_module[u'tasks'][i][u'name'] for i in module_tasks_iter]


    for i in module_task_list:
        if i not in data_module[u'dependencies'].keys():
            data_module[u'dependencies'][i] = []


    # unchanged_task_name = [l for l in module_task_list if l not in task_list]

    # 初始化一个数据结构data_real
    # jobname, schedule, notes取自data_module
    # tasks和dependencies都设置为空
    data_real = init_data_real(data_module)

    for task_id in module_tasks_iter:
        if data_module['tasks'][task_id]['name'] in task_list:
            for host in host_iter:
                update_data_real(data_module, data_real,
                                 task_id, host_list[host],
                                 task_list, module_tasks_iter)
        else:
            update_data_real2(data_module, data_real,
                              task_id, task_list,
                              host_iter, host_list,
                              module_tasks_iter)

        print u"Tasks: " + data_module['tasks'][task_id]['name'] + u" reconstruction complete!"

    # init session and post data to server
    jsonalize(data_real)

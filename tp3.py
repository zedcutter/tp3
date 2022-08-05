import sys,pprint,pandas as pd, glob, os
#--------------------------------------------------------------------------------------------           
# tp3 - simple template processor
# created by Zoltan Vago
# 2022, (c) GNU Licence
#
# It's a really simple tool, but it may come handy at some situations.
# If you look for a feature rich template engine opt for jinja2 or similar tools!
#
# how to call from command line (example):
#     python tp3.py template.sql sql_param
#
# output: 
#     Goes to <template file name>.<parameter set>.out, e.g.: "template.sql.sql_param.out"
#
# place your template into a templatefile, e.g. template.sql
# place your parameters into a "parameter set" built from more files: 
#     E.g.: your parameter set name is "sql_params"
#     - sql_param.VAR.variables (the VAR.variables ending is mandatory!)
#          - contains simple variables in format: 
#            VARIABLE = VALUE 
#     - sql_param.LIST.colinfo
#          - filename means: you will find a semicolon separated LIST, named "colinfo" 
#            and it belongs to the sql_param parameter set.
#          - first row of LIST is a semicolon-separated list of attribute names:
#            you will refer to the list items with these names! (e.g.: colinfo.column_name)
#          - If you need to use a semicolon in the attribute value, use \ as escape character
#     - sql_param.LIST.whatever
#          - it may contain another list of comma separated rows describing attributes of "whatever"
#
# variable references in the template body:
#   Simple variable:
#     {{VARNAME}} - gets replaced to VALUE of VARNAME
#
#   List iterator:      
#     {{{ colinfo::  
#		  {{!1::,}}  {{.colname}}	{{.datatype}}	{{.nullable}}
#	  }}}
#     Here you iterate a LIST with all the rows: in this case the colinfo list.
#     With ".colname" you reference the attribute of the default list (you're just iterating thru)
#     But you can explicitly add the prefix: {{colinfo.colname}}, as you prefer.
#
#   Skip condition:
#     You can use a special list construct in the {{{ ... }}} iterator body: 
#     {{!1::,}}      - Skip condition for first row of list: it will NOT place a "," (or
#                      whatever you put there) for the first item, but it will for all others.
#                      Useful for comma listed attributes in several programming languages.
#                      The only skip condition, now.
#
#   List filter:
#     {{{ colinfo::tabname={{stage_table_name}}              - By specifying an "attribute_name=value" right after the "::" 
#                                                              you can filter a list to the items where attribute_name 
#                                                              matches the value (similar to an SQL WHERE).
#                                                              The filtered value can also be a variable. Like here.
#
#--------------------------------------------------------------------------------------------           

#----------------------------------------------------
#FUNCTIONS
#----------------------------------------------------
def load_template_variables( filename ):
    #load variables from the VAR file

    try:
        f_var = open(filename,'r')
    except FileNotFoundError:
        print("Variable file {} does not exist. Exiting...".format(filename))
        sys.exit()
        
    lines = f_var.readlines() 
  
    cnt = 0
    for line in lines: 
        cnt=cnt+1 
        elements = line.split('=',2)
        tp_vars[elements[0]]=elements[1].rstrip()

    tp_vars['TP_VARCNT']=cnt


def load_template_params( param_set ):
    #Load all LISTs into separate pandas DataFrames
    #DF-s are loaded into a dictionary, where the key is the final tag of the filename after LIST.
    cnt=0
    varfiles = glob.glob(param_set+".LIST.*")
    
    for vf in varfiles:
        data = pd.read_csv(vf, sep=';', escapechar='\\') #, converters={'nullable' : str}
        fname_segments = vf.split('.',3)
        df_dictionary[fname_segments[2]]=data
        cnt=cnt+1
    return cnt

def replace_only_this_list(tpline,listname, domain, domainval):
    skip_value=False
    
    if tpline.find('{{')>-1 and tpline.find('}}')>-1 and tpline.find(listname+'.')>-1:
        linepart1=tpline[0:tpline.find('{{')]
        linepart2=tpline[tpline.find('}}')+2:len(tpline)]
        varname=tpline[tpline.find('{{')+2:tpline.find('}}')]
        
        #handle conditions if any
        if varname.find('::')>-1:
            condition=varname[0:varname.find('::')]             #extract condition
            varname=varname[varname.find('::')+2:len(varname)]  #finalize varname
            
            if condition=='!1' and top_list_cnt==0:
                skip_value=True
                
        if varname.find('.')>-1:
            #if dot found > LIST
            attrname=varname[varname.find('.')+1: len(varname)]
            #used list is either explicit or implicit
            if varname.find('.')==0:
                used_list=default_list
            else:
                used_list=varname[0:varname.find('.')]
               
            list_df_pre=df_dictionary[used_list]         
            if domain!='':
                is_in_domain = list_df_pre[domain]==domainval
                list_df=list_df_pre[is_in_domain]
            else:
                list_df=list_df_pre
            offset=list_df.index[0]
            
            #only replace if no skip condition added 
            if skip_value==False:
                try:
                    attr_val=str(list_df.at[top_list_cnt+offset,attrname])
                    if attr_val=='nan':
                        attr_val='';
                except KeyError:
                    attr_val=attrname;                 
                retval=linepart1+attr_val+linepart2
            else:
                retval=linepart1+linepart2
                if(debug==1):
                    print("retval at iteration "+str(top_list_cnt)+":"+retval)
        return replace_only_this_list(retval, listname, domain, domainval)
    else:
        return tpline
    

def replace_all_vars(tpline, default_list, domain, domainval):
    skip_value=False
    
    if tpline.find('{{')>-1 and tpline.find('}}')>-1:
        linepart1=tpline[0:tpline.find('{{')]
        linepart2=tpline[tpline.find('}}')+2:len(tpline)]
        varname=tpline[tpline.find('{{')+2:tpline.find('}}')]
        
        #handle conditions if any
        if varname.find('::')>-1:
            condition=varname[0:varname.find('::')]             #extract condition
            varname=varname[varname.find('::')+2:len(varname)]  #finalize varname
            
            if condition=='!1' and top_list_cnt==0:
                skip_value=True
                
        if varname.find('.')>-1:
            attrname=varname[varname.find('.')+1: len(varname)]
            #used list is either explicit or implicit
            if varname.find('.')==0:
                used_list=default_list
            else:
                used_list=varname[0:varname.find('.')]
               
            list_df_pre=df_dictionary[used_list]         
            if domain!='':
                is_in_domain = list_df_pre[domain]==domainval
                list_df=list_df_pre[is_in_domain]
                
            else:
                list_df=list_df_pre
            
            offset=list_df.index[0]
            
            #only replace if no skip condition added
            if skip_value==False:
                try:
                    attr_val=str(list_df.at[top_list_cnt+offset,attrname])
                    if(debug==1):
                        print("attr_val="+str(attr_val))
                        
                    if attr_val=='nan':
                        attr_val='';
                except KeyError:
                    attr_val=attrname;                 
                retval=linepart1+attr_val+linepart2
            else:
                retval=linepart1+linepart2
                
            if(debug==1):
                print("retval at iteration "+str(top_list_cnt)+":"+retval)
        else:
            #only replace if no skip condition added
            if skip_value==False:
                try:
                    var_val=tp_vars[varname]
                    if var_val=='nan':
                        var_val='';
                    retval=linepart1+var_val+linepart2
                except KeyError:
                    retval=linepart1+varname+linepart2
            else:
                retval=linepart1+linepart2
                
        return replace_all_vars(retval, default_list, domain, domainval)
    else:
        return tpline

def iterate_block(listname, curr_pos, domain, domainval):
    global outlist
    global top_list_cnt
    retval=False
    end_of_file=False
    
    df_pre=df_dictionary[listname]
    if domain!='':
        is_in_domain = df_pre[domain]==domainval
        df=df_pre[is_in_domain]
    else:
        df=df_pre
                
    if(debug==1):
        print("The list will be used:")            
        print(df)
            
    listsize=len(df)

    for i in range(listsize):
        start_pos=curr_pos+1
        poscnt=0
        depth=0
        end_of_file=False
        top_list_cnt=i

        try:
            block_line=inlist[start_pos+poscnt]
        except IndexError:
            end_of_file=True
            block_line='NO BLOCK LINE AT: '+str(start_pos)
        
        while block_line and depth>-1 and end_of_file==False:
            if(debug==1):
                print("LIST="+listname)
                print("    iterating, block line processed: "+block_line)
                print("    rec in block="+str(i))
                print("    local depth="+str(depth))
                print("    ----------")
            if block_line.strip().find('{{{')>-1:
                depth+=1
                retval=True
            elif block_line.strip().find('}}}')>-1:
                depth-=1
            
            if depth==0:
                outlist.append(replace_all_vars(block_line,listname,domain,domainval))
            elif depth>0:
                outlist.append(replace_only_this_list(block_line,listname, domain, domainval))
            
            poscnt+=1
            try:
                block_line=inlist[start_pos+poscnt]
            except IndexError:
                end_of_file=True

    return retval
    
    
def process_inlist():
    global outlist
    global depth
    global listname
    
    retval=False
    inlist_current_pos=-1
    inlistsize=len(inlist)
    still_processing=True
    
    while still_processing==True:
        inlist_current_pos+=1
        if inlist_current_pos<inlistsize:
            tpline=inlist[inlist_current_pos]

            #process a single line of the template
            if tpline.strip().find('{{{')>-1:
                depth+=1
                listname=replace_all_vars(tpline[tpline.find('{{{')+3:tpline.find('::')].strip(),'NOLIST','','')             
                domainfilter=tpline[tpline.find('::')+2:len(tpline)]
                domain=domainfilter[0:domainfilter.find('=')]
                domainval=domainfilter[domainfilter.find('=')+1:len(domainfilter)]
                domainval=replace_all_vars(domainval,'NOLIST','','').strip()
                
                if(debug==1):
                    print("domain="+domain)
                    print("domainval="+domainval)
                    
                if depth==0:
                    retval=iterate_block(listname, inlist_current_pos, domain, domainval)
                
            elif tpline.strip().find('}}}')>-1:
                #drop top listheap element
                depth-=1
                
            else:
                if depth==-1:
                    outlist.append( replace_all_vars(tpline, listname,'','') )
        else:
            still_processing=False
            
    return retval
    
def validate_listname(lname):
    retval=False
    for key in df_dictionary:
        if lname==key:
            retval=True
    return retval

def validate_list_attr(lname, aname):
    retval=False
    
    retval=validate_listname(lname)
    if retval==True:
        df=df_dictionary[lname]
        for a in df.columns:
            if aname==a:
                retval=True                
    return retval
    
def validate_var(var):
    retval=False
    for v in tp_vars:
        if v==var:
            retval=True
    return retval
  
def validate_template_line(idx,l,p_depth,p_iter):
    global v_err, default_list
    
    retval=True
    depth=p_depth
    
    if l.strip().find('{{{')>-1:
        depth+=1
        default_list=l[l.find('{{{')+3:l.find('::')].strip()
        if default_list.find('{{')>-1:
            varname=default_list[default_list.find('{{')+2:default_list.find('}}')]
            rv=validate_var(varname)
            if rv==False:
                print('ERRCODE-1001. No such variable: "{}" at template line {}, position {}'.format(varname,idx+1,p_iter))
                retval=False
                v_err+=1;
            else:
                default_list=default_list[0:default_list.find('{{')]+tp_vars[varname]+default_list[default_list.find('}}')+2:len(default_list)]
        
        rv=validate_listname(default_list)
        if rv==False:
            print('ERRCODE-1002. No such list: "{}" at template line {}, position {}'.format(default_list,idx+1,p_iter))
            retval=False
            v_err+=1;
            
        if debug==1:
            print("ERRCODE-1003. default_list after variable replacement: "+default_list)
        list_name_stack.append(default_list)
        
    elif l.strip().find('}}}')>-1:
        #drop top listheap element
        depth-=1
        list_name_stack.pop()
        try:
            default_list=list_name_stack[len(list_name_stack)]
        except IndexError:
            default_list=''
        
    elif l.strip().find('{{')>-1 and l.strip().find('}}')>-1:
        varstr=l[l.find('{{')+2:l.find('}}')]
        if debug==1:
            print("ERRCODE-1004. VAR detected at line {}, varstr={}".format(idx+1,varstr))
            
        #LIST ATTR validation
        if varstr.find('.')>-1:
            if debug==1:
                print("ERRCODE-1005. LIST_ATTR validation, varstr="+varstr+" ;default_list="+default_list)
            attr_name=varstr[varstr.find('.')+1:len(varstr)]
            if varstr[0]=='.':
                list_name=default_list
            else:
                list_name=varstr[0:varstr.find('.')]
            rv=validate_list_attr(list_name, attr_name)
            if rv==False:
                print('ERRCODE-1006. No such list attribute: "{}.{}" at template line {}, position {}'.format(list_name,attr_name,idx+1,p_iter))
                retval=False
                v_err+=1;
        elif varstr[0:4]=='!1::':
                retval=retval 
        else:
            rv=validate_var(varstr)
            if rv==False:
                print('ERRCODE-1007. No such variable: "{}" at template line {}, position {}'.format(varstr,idx+1,p_iter))
                retval=False
                v_err+=1;
    
    if l.find('}}')>-1:
        restofline=l[l.find('}}')+2:len(l)]
        rv=validate_template_line(idx,restofline,depth,p_iter+1)
        if rv==False:
            retval=False
            v_err+=1;

    return retval

def validate_template():
    retval=True
    rv=True
    depth=-1
    
    print("Validating template...")
    for idx, l in enumerate(inlist):
        retval=validate_template_line(idx,l,depth,1)
    
    if debug==1:
        print("Validation return value is "+str(retval))
        print("Nr of validation errors: "+str(v_err))
       
    if v_err>0:
        retval=False
        
    return retval   
                    
                
                
       
#--------------------------------------------------------------------------------------------           
#BEGIN   
#--------------------------------------------------------------------------------------------           

#----------------------------------------------------
#validate command line parameters
#----------------------------------------------------
param_cnt=len(sys.argv)-1

if param_cnt<2:
    print("Please specify at least P1 (templatefile) and P2 (parameterfile-pattern)!")
    quit()
    
templatefile=str(sys.argv[1])
param_set=str(sys.argv[2])

#----------------------------------------------------
#set up global lists and variables
#----------------------------------------------------
debug=0
paramfiles=list()
tp_vars = {'TP_VARCNT':0}
df_dictionary={'TP_FILECNT':0}
top_list_cnt=-1
depth=-1
listname=''
list_name_stack=list()
list_cnt_stack=list()
block_lines=list()
puffer_stack=list()
v_err=0
default_list=''


inlist=list()
outlist=list()
remained_any_list=True

#----------------------------------------------------
#load template variables and parameters
#----------------------------------------------------
load_template_variables(param_set+".VAR.variables")
lfcnt=load_template_params(param_set)
df_dictionary['TP_FILECNT']=lfcnt

#----------------------------------------------------
#open and process the template
#----------------------------------------------------

#initial load of inlist
f_tp_in = open(templatefile,'r')
template_line = f_tp_in.readline()
while template_line:
    inlist.append(template_line)
    template_line=f_tp_in.readline()
f_tp_in.close()

if validate_template()==False:
    print("Template validation failed. Exiting...")
    quit()
    
#iterative process of inlist
while remained_any_list==True:
    #print("Starting iteration...")
    remained_any_list=process_inlist()
    
    #remained_any_list=False #test 1 iter only
    
    if remained_any_list:
        inlist.clear()
        #print("Next inlist is this outlist:")
        for l in outlist:
            inlist.append(l)
            #print(l)
        outlist.clear()   

#create output from final outlist
f_tp_out = open(templatefile+"."+param_set+".out",'w')
for line in outlist:
    f_tp_out.write( line )
f_tp_out.close()

print("Template processed. See output in: "+templatefile+"."+param_set+".out")

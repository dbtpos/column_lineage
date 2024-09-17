import streamlit as st
import snowflake.connector
from pandas import DataFrame
from streamlit_option_menu import option_menu
from streamlit_dbtree import streamlit_dbtree
import pandas as pd
from ydata_profiling import ProfileReport
#import ipywidgets
#from pydantic_settings import BaseSettings
import streamlit.components.v1 as components
from IPython.core.display import display,HTML
from bs4 import BeautifulSoup
import time
from snowflake.connector.pandas_tools import write_pandas
import configparser, re, json, os
from snowflake.snowpark import Session
import toml
from streamlit_float import *


def load_view():
    st.title('Data Page')

float_init()

if "show" not in st.session_state:
    st.session_state.show = True

def read_html_with_beautiful_soup(file_path):
    # Read HTML file
    with open(file_path, 'r') as f:
        # Parse HTML using BeautifulSoup
        soup = BeautifulSoup(f, 'html.parser')
    # Find all tables in the HTML
    tables = soup.find_all('table')
    # Read tables into DataFrame using read_html()
    df = pd.read_html(str(tables))[0]
    return df
#import pandas_profiling as pp

#st.set_page_config(page_title="Snowflake Database Portal", page_icon=":tada:", layout="wide")

with st.popover("Open popover"):
    st.markdown("Hello World 👋")
    name = st.text_input("What's your name?")
    address = st.text_input("What's your Address?")
st.write("Your name:", name)
st.write("Your Address:", address)

conn = st.connection("snowflake")
cur = conn.cursor()
db_name=cur.execute("select current_database()").fetchone()[0]
schema_name = cur.execute("select current_schema()").fetchone()[0]
selected2 = option_menu(None, ["Home", "Upload", "Tasks", 'Settings',"Help",], 
    icons=['house', 'cloud-upload', "list-task", 'gear','question-square-fill'], 
    menu_icon="cast", default_index=0, orientation="horizontal")
#selected2
col1,col2,col3 = st.columns([0.05,0.5,1])
label = r'''$\textsf{\large Snowflake DB Portal}$'''

with col1:

    with st.sidebar:
        #st.image("https://www.snowflake.com/wp-content/uploads/2022/03/SOLAR_Blog.png",use_column_width='auto')
        selected_menu = option_menu(menu_title=st.write(label), options=["DBTree", "Database Info",  "Database Users", "Executed SQLs","Storage Usage","Review Form"], 
        icons=['house', 'cloud-upload', "list-task", 'gear','database','receipt'],orientation="vertical")
if selected_menu == "DBTree":
 with col2:
        title = r'''$\textsf{\large  Snowflake DB Structure}$'''    
        st.write('❄️'+ title)
        #st.write(" This app is to display information about your Snowflake database.")   
        value = streamlit_dbtree(conn)



        if value is not None:
            for sel in value:
                st.write(sel.get("id") +" IS A " +sel.get("type"))
                table_name=sel.get("id")
                with col3:
         
                    st.subheader(r'''$\textsf{\normalsize  Workarea}$''')
                    # Create tabs
                    tab_titles = ['Metadata', 'Data', 'Associations', 'SQLs','Visualization']
                    tabs = st.tabs(tab_titles)

   
                    with tabs[0]:
                        st.header('Metadata')
                        st.write('Below is the Metadata for '+table_name)
                        df = conn.query("select t1.*,t2.*exclude(COLUMN_NAME),t3.*exclude(REF_COLUMN_NAME) From " + db_name +".INFORMATION_SCHEMA.COLUMNS t1 left join (select TAG_NAME,TAG_VALUE,COLUMN_NAME from table("+db_name+".information_schema.tag_references_all_columns('"+table_name +"', 'table')))t2 on t1.Column_name = t2.Column_name left join (select POLICY_NAME,POLICY_KIND,REF_COLUMN_NAME,POLICY_STATUS from table(information_schema.policy_references(ref_entity_name => '"+table_name+"',ref_entity_domain => 'Table')))t3 On t1.Column_Name = t3.REF_COLUMN_NAME where CONCAT_WS('.',t1.TABLE_CATALOG,t1.TABLE_SCHEMA,t1.TABLE_NAME) = '"+table_name+"'")
                        df=pd.DataFrame(df)
                        filter = st.multiselect('Data_Type_Filter',df['DATA_TYPE'].unique())
                        if filter == []:
                            new_df = df
                        else:
                            new_df = df[df.DATA_TYPE.isin(filter)]
                        st.dataframe(new_df)
                    
                    with tabs[1]:
                        st.header('Data')
                        st.write('Below is the Data for '+table_name)
                        df = conn.query("select * from " + table_name)
                        df.columns = df.columns.str.replace('Price', 'Price 💲')
                        profile = ProfileReport(df,title = "Trending_Stocks")
                        #st.write(df)
                        with st.form("data_editor_form"):
                            edited = st.data_editor(df, use_container_width=True, num_rows="dynamic")
                            submit_button = st.form_submit_button("Submit")
                        if submit_button:
                            try:
                                write_pandas(conn,edited, table_name.split(".")[-1])
                                #st.write(edited)
                                st.success("Table updated")
                                time.sleep(5)
                            except:
                                st.warning("Error updating table")
                                time.sleep(5)
                            st.rerun()
                            
                        def convert_df(df):
                        # IMPORTANT: Cache the conversion to prevent computation on every rerun
                            return df.to_csv().encode('utf-8')
                        csv = convert_df(df)
                        st.download_button(
                        label="Download .CSV",
                        data=csv,
                        file_name=table_name.split(".")[-1]+'.csv',
                        mime='text/csv',)

                        my_report = profile.to_file("my_report.html")
                        html_file_path = 'my_report.html'
                        # Read HTML file using BeautifulSoup with read_html()
                        df_profile = read_html_with_beautiful_soup(html_file_path)
                        st.subheader('Data Profile')
                        st.write('#Number of variables => Number of Columns')
                        st.write('#Number of observations => Number of Rows')
                        st.write(df_profile)
                        #st.write(pd.read_html("my_report.html"))
                        
            
                    with tabs[2]:
                        st.header('Associations')
                        if 'snowflake_connection' not in st.session_state:
                            # connect to Snowflake
                            with open('.streamlit/secrets.toml') as f:
                                connection_parameters = toml.load(f)
                            st.session_state.snowflake_connection = Session.builder.configs(connection_parameters["connections"]["snowflake"]).create()
                            session = st.session_state.snowflake_connection
                        else:
                            session = st.session_state.snowflake_connection
                        class Theme:
                            def __init__(self, color, fillcolor, fillcolorC,
                                    bgcolor, icolor, tcolor, style, shape, pencolor, penwidth):
                                self.color = color
                                self.fillcolor = fillcolor
                                self.fillcolorC = fillcolorC
                                self.bgcolor = bgcolor
                                self.icolor = icolor
                                self.tcolor = tcolor
                                self.style = style
                                self.shape = shape
                                self.pencolor = pencolor
                                self.penwidth = penwidth
                        class Table:
                            def __init__(self, name, comment):
                                self.name = name
                                self.comment = comment if comment is not None and comment != 'None' else ''
                                self.label = None

                                self.columns = []           # list of all columns
                                self.uniques = {}           # dictionary with UNIQUE constraints, by name + list of columns
                                self.pks = []               # list of PK columns (if any)
                                self.fks = {}               # dictionary with FK constraints, by name + list of FK columns


                            @classmethod
                            def getClassName(cls, name, useUpperCase, withQuotes=True):
                                if re.match("^[A-Z_0-9]*$", name) == None:
                                    return f'"{name}"' if withQuotes else name
                                return name.upper() if useUpperCase else name.lower()

                            def getName(self, useUpperCase, withQuotes=True):
                                return Table.getClassName(self.name, useUpperCase, withQuotes)


                            def getColumn(self, name):
                                for column in self.columns:
                                    if column.name == name:
                                        return column
                                return None


                            def getUniques(self, name, useUpperCase):
                                constraint = self.uniques[name]
                                uniques = [column.getName(useUpperCase) for column in constraint]
                                ulist = ", ".join(uniques)

                                if useUpperCase:
                                    return (f',\n  CONSTRAINT {Table.getClassName(name, useUpperCase)}\n'
                                        + f"    UNIQUE ({ulist})")
                                return (f',\n  constraint {Table.getClassName(name, useUpperCase)}\n'
                                    + f"    unique ({ulist})")


                            def getPKs(self, useUpperCase):
                                pks = [column.getName(useUpperCase) for column in self.pks]
                                pklist = ", ".join(pks)
                                pkconstraint = self.pks[0].pkconstraint

                                if useUpperCase:
                                    return (f',\n  CONSTRAINT {Table.getClassName(pkconstraint, useUpperCase)}\n'
                                        + f"    PRIMARY KEY ({pklist})")
                                return (f',\n  constraint {Table.getClassName(pkconstraint, useUpperCase)}\n'
                                    + f"    primary key ({pklist})")


                            def getFKs(self, name, useUpperCase):
                                constraint = self.fks[name]
                                pktable = constraint[0].fkof.table

                                fks = [column.getName(useUpperCase) for column in constraint]
                                fklist = ", ".join(fks)
                                pks = [column.fkof.getName(useUpperCase) for column in constraint]
                                pklist = ", ".join(pks)

                                if useUpperCase:
                                    return (f"ALTER TABLE {self.getName(useUpperCase)}\n"
                                        + f"  ADD CONSTRAINT {Table.getClassName(name, useUpperCase)}\n"
                                        + f"  ADD FOREIGN KEY ({fklist})\n"
                                        + f"  REFERENCES {pktable.getName(useUpperCase)} ({pklist});\n\n")
                                return (f"alter table {self.getName(useUpperCase)}\n"
                                    + f"  add constraint {Table.getClassName(name, useUpperCase)}\n"
                                    + f"  add foreign key ({fklist})\n"
                                    + f"  references {pktable.getName(useUpperCase)} ({pklist});\n\n")


                            # outputs a CREATE TABLE statement for the current table
                            def getCreateTable(self, useUpperCase):
                                if useUpperCase:
                                    s = f"CREATE OR REPLACE TABLE {self.getName(useUpperCase)} ("
                                else:
                                    s = f"create or replace table {self.getName(useUpperCase)} ("
                                
                                first = True
                                for column in self.columns:
                                    if first: first = False
                                    else: s += ","
                                    s += column.getCreateColumn(useUpperCase)

                                if len(self.uniques) > 0:
                                    for constraint in self.uniques:
                                        s += self.getUniques(constraint, useUpperCase)
                                if len(self.pks) >= 1:
                                    s += self.getPKs(useUpperCase)
                                
                                s += "\n)"
                                if self.comment != '':
                                    comment = self.comment.replace("'", "''")
                                    s += f" comment = '{comment}'" if not useUpperCase else f" COMMENT = '{comment}'"
                                return s + ";\n\n"


                            def getDotShape(self, theme, showColumns, showTypes, useUpperCase):
                                fillcolor = theme.fillcolorC if showColumns else theme.fillcolor
                                colspan = "2" if showTypes else "1"
                                tableName = self.getName(useUpperCase, False)
                                s = (f'  {self.label} [\n'
                                    + f'    fillcolor="{fillcolor}" color="{theme.color}" penwidth="1"\n'
                                    + f'    label=<<table style="{theme.style}" border="0" cellborder="0" cellspacing="0" cellpadding="1">\n'
                                    + f'      <tr><td bgcolor="{theme.bgcolor}" align="center"'
                                    + f' colspan="{colspan}"><font color="{theme.tcolor}"><b>{tableName}</b></font></td></tr>\n')

                                if showColumns:
                                    for column in self.columns:
                                        datatype_icon = {"varchar":"🔤","int":"🔢","timestamp":"⏱️"}
                                        name = column.getName(useUpperCase, False)
                                        if 'varchar' in column.datatype:  name= name+" "+datatype_icon["varchar"]
                                        if 'int' in column.datatype: name = name+" "+datatype_icon["int"]
                                        if 'timestamp' in column.datatype: name = name+" "+datatype_icon["timestamp"]
                                        if column.ispk: name = f"<u>{name}</u>"
                                        if column.fkof != None: name = f"<i>{name}</i>"
                                        if column.nullable: name = f"{name}*"
                                        if column.identity: name = f"{name} I"
                                        if column.isunique: name = f"{name} U"
                                        datatype = column.datatype
                                        if useUpperCase: datatype = datatype.upper()

                                        if showTypes:
                                            s += (f'      <tr><td align="left"><font color="{theme.icolor}">{name}&nbsp;</font></td>\n'
                                                + f'        <td align="left"><font color="{theme.icolor}">{datatype}</font></td></tr>\n')
                                        else:
                                            s += f'      <tr><td align="left"><font color="{theme.icolor}">{name}</font></td></tr>\n'

                                return s + '    </table>>\n  ]\n'


                            def getDotLinks(self, theme):
                                s = ""
                                for constraint in self.fks:
                                    fks = self.fks[constraint]
                                    fk1 = fks[0]
                                    dashed = "" if not fk1.nullable else ' style="dashed"'
                                    arrow = "" if fk1.ispk and len(self.pks) == len(fk1.fkof.table.pks) else ' arrowtail="crow"'
                                    s += (f'  {self.label} -> {fk1.fkof.table.label}'
                                        + f' [ penwidth="{theme.penwidth}" color="{theme.pencolor}"{dashed}{arrow} ]\n')
                                return s    
                        class Column:
                            def __init__(self, table, name, comment):
                                self.table = table
                                self.name = name
                                self.comment = comment if comment is not None and comment != 'None' else ''
                                self.nullable = True
                                self.datatype = None        # with (length, or precision/scale)
                                self.identity = False

                                self.isunique = False
                                self.ispk = False
                                self.pkconstraint = None
                                self.fkof = None            # points to the PK column on the other side


                            def getName(self, useUpperCase, withQuotes=True):
                                return Table.getClassName(self.name, useUpperCase, withQuotes)


                            def setDataType(self, datatype):
                                self.datatype = datatype["type"]
                                self.nullable = bool(datatype["nullable"])

                                if self.datatype == "FIXED":
                                    self.datatype = "NUMBER"
                                elif "fixed" in datatype:
                                    fixed = bool(datatype["fixed"])
                                    if self.datatype == "TEXT":
                                        self.datatype = "CHAR" if fixed else "VARCHAR"

                                if "length" in datatype:
                                    self.datatype += f"({str(datatype['length'])})"
                                elif "scale" in datatype:
                                    if int(datatype['precision']) == 0:
                                        self.datatype += f"({str(datatype['scale'])})"
                                        if self.datatype == "TIMESTAMP_NTZ(9)":
                                            self.datatype = "TIMESTAMP"
                                    elif "scale" in datatype and int(datatype['scale']) == 0:
                                        self.datatype += f"({str(datatype['precision'])})"
                                        if self.datatype == "NUMBER(38)":
                                            self.datatype = "INT"
                                        elif self.datatype.startswith("NUMBER("):
                                            self.datatype = f"INT({str(datatype['precision'])})"
                                    elif "scale" in datatype:
                                        self.datatype += f"({str(datatype['precision'])},{str(datatype['scale'])})"
                                        #if column.datatype.startswith("NUMBER("):
                                        #    column.datatype = f"FLOAT({str(datatype['precision'])},{str(datatype['scale'])})"
                                self.datatype = self.datatype.lower()


                            # outputs the column definition in a CREATE TABLE statement, for the parent table
                            def getCreateColumn(self, useUpperCase):
                                nullable = "" if self.nullable or (self.ispk and len(self.table.pks) == 1) else " not null"
                                if useUpperCase: nullable = nullable.upper()
                                identity = "" if not self.identity else " identity"
                                if useUpperCase: identity = identity.upper()
                                pk = ""     # if not self.ispk or len(self.table.pks) >= 2 else " primary key"
                                if useUpperCase: pk = pk.upper()
                                datatype = self.datatype
                                if useUpperCase: datatype = datatype.upper()
                                
                                comment = self.comment.replace("'", "''")
                                if comment != '': comment = f" COMMENT '{comment}'" if useUpperCase else f" comment '{comment}'"

                                return f"\n  {self.getName(useUpperCase)} {datatype}{nullable}{identity}{pk}{comment}"



                        def importMetadata(database, schema):
                            global session
                            tables = {}
                            if database == '' or schema == '': return tables
                            suffix = f"in schema {Table.getClassName(database, False)}.{Table.getClassName(schema, False)}"

                            # get tables
                            query = f"show tables {suffix}"
                            st.write(query)
                            results = session.sql(query).collect()
                            #st.dataframe(results)
                            #st.write(results[0])
                            for row in results:
                                #st.write(row)
                                tableName = str(row["name"])
                                table = Table(tableName, str(row["comment"]))
                                tables[tableName] = table
                                table.label = f"n{len(tables)}"

                            # get table columns
                            query = f"show columns {suffix}"
                            results = session.sql(query).collect()
                            for row in results:
                                tableName = str(row["table_name"])
                                if tableName in tables:
                                    table = tables[tableName]

                                    name = str(row["column_name"])
                                    column = Column(table, name, str(row["comment"]))
                                    table.columns.append(column)

                                    column.identity = str(row["autoincrement"]) != ''
                                    column.setDataType(json.loads(str(row["data_type"])))

                            # get UNIQUE constraints
                            query = f"show unique keys {suffix}"
                            results = session.sql(query).collect()
                            for row in results:
                                tableName = str(row["table_name"])
                                if tableName in tables:
                                    table = tables[tableName]
                                    column = table.getColumn(str(row["column_name"]))

                                    # add a UNIQUE constraint (if not there) with the current column
                                    constraint = str(row["constraint_name"])
                                    if constraint not in table.uniques:
                                        table.uniques[constraint] = []
                                    table.uniques[constraint].append(column)
                                    column.isunique = True

                            # get PKs
                            query = f"show primary keys {suffix}"
                            results = session.sql(query).collect()
                            for row in results:
                                tableName = str(row["table_name"])
                                if tableName in tables:
                                    table = tables[tableName]
                                    column = table.getColumn(str(row["column_name"]))
                                    column.ispk = True
                                    column.pkconstraint = str(row["constraint_name"])

                                    pos = int(row["key_sequence"]) - 1
                                    table.pks.insert(pos, column)

                            # get FKs
                            query = f"show imported keys {suffix}"
                            results = session.sql(query).collect()
                            for row in results:
                                pktableName = str(row["pk_table_name"])
                                fktableName = str(row["fk_table_name"])
                                if pktableName in tables and fktableName in tables:
                                    pktable = tables[pktableName]
                                    pkcolumn = pktable.getColumn(str(row["pk_column_name"]))
                                    fktable = tables[fktableName]
                                    fkcolumn = fktable.getColumn(str(row["fk_column_name"]))

                                    # add a constraint (if not there) with the current FK column
                                    if str(row["pk_schema_name"]) == str(row["fk_schema_name"]):
                                        constraint = str(row["fk_name"])
                                        if constraint not in fktable.fks:
                                            fktable.fks[constraint] = []
                                        fktable.fks[constraint].append(fkcolumn)

                                        fkcolumn.fkof = pkcolumn
                                        #print(f"{fktable.name}.{fkcolumn.name} -> {pktable.name}.{pkcolumn.name}")
                            
                            return tables

                        def createScript(tables, database, schema, useUpperCase):
                            db = Table.getClassName(database, useUpperCase)
                            sch = f'{db}.{Table.getClassName(schema, useUpperCase)}'
                            if useUpperCase: s = f"USE DATABASE {db};\nCREATE OR REPLACE SCHEMA {sch};\n\n"
                            else: s = f"use database {db};\ncreate or replace schema {sch};\n\n"

                            for name in tables:
                                s += tables[name].getCreateTable(useUpperCase)
                            for name in tables:
                                for constraint in tables[name].fks:
                                    s += tables[name].getFKs(constraint, useUpperCase)
                            return s

                        

                        def createGraph(tables, theme, showColumns, showTypes, useUpperCase):
                            s = ('digraph {\n'
                                + '  graph [ rankdir="LR" bgcolor="#ffffff" ]\n'
                                + f'  node [ style="filled" shape="{theme.shape}" gradientangle="180" ]\n'
                                + '  edge [ arrowhead="none" arrowtail="none" dir="both" ]\n\n')

                            for name in tables:
                                s += tables[name].getDotShape(theme, showColumns, showTypes, useUpperCase)
                            s += "\n"
                            for name in tables:
                                s += tables[name].getDotLinks(theme)
                            s += "}\n"
                            return s

                        def getThemes():
                            return {
                                "Common Gray": Theme("#6c6c6c", "#e0e0e0", "#f5f5f5",
                                    "#e0e0e0", "#000000", "#000000", "rounded", "Mrecord", "#696969", "1"),
                                "Blue Navy": Theme("#1a5282", "#1a5282", "#ffffff",
                                    "#1a5282", "#000000", "#ffffff", "rounded", "Mrecord", "#0078d7", "2"),
                                #"Gradient Green": Theme("#716f64", "#008080:#ffffff", "#008080:#ffffff",
                                #    "transparent", "#000000", "#000000", "rounded", "Mrecord", "#696969", "1"),
                                #"Blue Sky": Theme("#716f64", "#d3dcef:#ffffff", "#d3dcef:#ffffff",
                                #    "transparent", "#000000", "#000000", "rounded", "Mrecord", "#696969", "1"),
                                "Common Gray Box": Theme("#6c6c6c", "#e0e0e0", "#f5f5f5",
                                    "#e0e0e0", "#000000", "#000000", "rounded", "record", "#696969", "1")
                            }   

                        def getDatabase():
                            global session
                            names = []
                            query = "show databases"
                            results = session.sql(query).collect()
                            for row in results:
                                names.append(str(row["name"]))
                            sel = 0 if "Chinook" not in names else names.index("Chinook")
                            return st.sidebar.selectbox('Database', tuple(names), index=sel,
                                help="Select an existing database")


                        def getSchema(database):
                            global session
                            names = []
                            if database != "":
                                query = f"show schemas in database {Table.getClassName(database, False)}"
                                results = session.sql(query).collect()
                                for row in results:
                                    schemaName = str(row["name"])
                                    if schemaName != "INFORMATION_SCHEMA":
                                        names.append(schemaName)
                            sel = 0 if "PUBLIC" not in names else names.index("PUBLIC")
                            return st.sidebar.selectbox('Schema', tuple(names), index=sel, 
                                help="Select a schema for the current database")

                        #st.set_page_config(layout="wide")
                        #session = getSession()
                        themes = getThemes()
                        database = table_name.split(".")[0]
                        schema = table_name.split(".")[1]
                        #conn = st.connection("snowflake")
                        #cur = conn.cursor()

                        #database =cur.execute("select current_database()").fetchone()[0]
                        #schema = cur.execute("select current_schema()").fetchone()[0]

                        #st.sidebar.divider()
                        theme = st.selectbox('Theme', tuple(themes.keys()), index=0, 
                            help="Select another color theme")
                        showColumns = st.checkbox('Display Column Names', value=False, 
                            help="Show columns in the expanded table shapes")
                        showTypes = st.checkbox('Display Data Types', value=False, 
                            help="Show column data type names when the table shapes are expanded")
                        useUpperCase = st.checkbox('Use Upper Case', value=False, 
                            help="Use upper case for table, column, and data type names, and for the script keywords")

                        with st.spinner('Reading metadata...'):
                            tables = importMetadata(database, schema)
                        if database == '' or schema == '':
                            st.write("Select a database and a schema.")
                        elif len(tables) == 0:
                            st.write("Found no tables in the current database and schema.")
                        else:
                            with st.spinner('Generating diagram and script...'):
                                graph = createGraph(tables, themes[theme], showColumns, showTypes, useUpperCase)
                                script = createScript(tables, database, schema, useUpperCase)

                                tabERD, tabScript = st.tabs(["ERD Viewer", "Create Script"])
                                tabERD.graphviz_chart(graph, use_container_width=False)
                                #tabDOT.code(graph, language="dot", line_numbers=True)
                                tabScript.code(script, language="sql", line_numbers=True)    
                        #df = conn.query("select rc.CONSTRAINT_SCHEMA as fk_schema,tc_parent.table_name as parent_table,tc_child.table_name as child_table,rc.constraint_name as child_fk_name,rc.unique_constraint_name as parent_constraint_name,tc_parent.constraint_type as parent_constraint_type,rc.created as fk_created from information_schema.REFERENTIAL_CONSTRAINTS rc join information_schema.TABLE_CONSTRAINTS tc_parent on rc.UNIQUE_CONSTRAINT_NAME = tc_parent.CONSTRAINT_NAME and rc.CONSTRAINT_SCHEMA = tc_parent.CONSTRAINT_SCHEMA join information_schema.TABLE_CONSTRAINTS tc_child on rc.CONSTRAINT_NAME = tc_child.CONSTRAINT_NAME and rc.CONSTRAINT_SCHEMA = tc_child.CONSTRAINT_SCHEMA")
                        #st.write(df) 
                    with tabs[3]:
                        st.header('SQLs')
                        df = conn.query("select get_ddl('Table'," + "'" + table_name + "') as DDL")
                        st.dataframe(df,width = 900)
                        #st.write(df_query)

                    with tabs[4]:
                        st.header('Visualization')
                        
                        df9= conn.query("select * from  "+table_name+"")
                        #my_large_df= conn.query("SELECT * FROM "+selectbox1+"."+selectbox2+"."+selectbox3+"")
                        
        
                        

                        df7= conn.query("select distinct column_name from information_schema.columns where table_name = '"+table_name.split(".")[-1]+"'")
                        selectbox4= st.selectbox('Select col1 x axis', df7)

                        df8= conn.query("select distinct column_name from information_schema.columns where table_name = '"+table_name.split(".")[-1]+"'")
                        selectbox5= st.selectbox('Select col2 y axis', df8)
        
                        selectbox6= st.selectbox('Select Visulaization', ('bar chart','line'))
    
                        if selectbox6 == "bar chart":
                            st.bar_chart(df9, x=selectbox4, y= selectbox5 )

                        elif  selectbox6 == "line":  
                            st.line_chart(df9,  x=selectbox4, y= selectbox5)  

            

 

elif selected_menu == "Database Info":
        st.title("Database Info")
        db_name=cur.execute("select current_database()").fetchone()[0]
        time = cur.execute("select current_timestamp()").fetchone()[0]
        ware = cur.execute("select current_warehouse()").fetchone()[0]
        ver = cur.execute("select current_version()").fetchone()[0]
        DB_NAME, TIME_STAMP,WAREHOUSE_NAME,VERSION = st.columns(4)
        with DB_NAME:
            st.subheader("Database Name")
            st.write(db_name)
        with TIME_STAMP:
            st.subheader("Time")
            st.write(time)
        with WAREHOUSE_NAME:
            st.subheader("Warehouse Name")
            st.write(ware)
        with VERSION:
            st.subheader("Snowflake Version")
            st.write(ver)
elif selected_menu == "Database Users":
        st.title("Snowflake users")
        users = cur.execute("select * from snowflake.account_usage.users").fetchall()
        names = [ x[0] for x in cur.description]
        df = DataFrame(users,columns=names)
        st.dataframe(df)
elif selected_menu == "Executed SQLs":
        st.title("Snowflake Executed SQL's")
        sqls = cur.execute("select * from snowflake.account_usage.query_history order by start_time").fetchall()
        col1 = [ x[0] for x in cur.description]
        df2 = DataFrame(sqls,columns=col1)
        st.dataframe(df2)
elif selected_menu == "Storage Usage":
        st.title("Snowflake Database Storage: ")
        storage = cur.execute("select * from snowflake.account_usage.storage_usage;").fetchall()
        col2 = [ x[0] for x in cur.description]
        df3 = DataFrame(storage,columns=col2)
        st.dataframe(df3)

elif selected_menu == "Review Form":
        with st.form("update_report",clear_on_submit=True):
            rating = st.slider('Rate Us :smile:', 0,5)
            comment_txt = st.text_area('Customer Review:')
            comment_dt = st.date_input('Date of report:')
            col1,col2 = st.columns([0.1,0.1])
            with col1:
             sub_comment = st.form_submit_button('Submit')
            with col2:
             Get_Graph = st.form_submit_button('Display Graph')
            st.write(comment_txt)
     
        
        
         
            if sub_comment:
             cur.execute("insert into "+db_name+"."+schema_name+".CUSTOMER_REVIEW (CUSTOMER_REVIEW,RATING,SUBMITED_ON) VALUES ('"+comment_txt+"','"+str(rating)+"','"+str(comment_dt)+"')").fetchall()
             st.success('Success!', icon="✅")
        
         
            if Get_Graph:    
             df = conn.query("SELECT rating,count(*) as rating_count FROM "+db_name+"."+schema_name+".CUSTOMER_REVIEW group by rating")
             st.write(df)
             # Create a bar chart
             st.bar_chart(df, x="RATING", y="RATING_COUNT")

# Container with expand/collapse button
button_container = st.container()
with button_container:
    if st.session_state.show:
        if st.button("⭳", type="primary"):
            st.session_state.show = False
            st.experimental_rerun()
    else:
        if st.button("⭱", type="secondary"):
            st.session_state.show = True
            st.experimental_rerun()
    
# Alter CSS based on expand/collapse state
if st.session_state.show:
    vid_y_pos = "2rem"
    button_css = float_css_helper(width="2.2rem", right="2rem", bottom="21rem", transition=0)
else:
    vid_y_pos = "-19.5rem"
    button_css = float_css_helper(width="2.2rem", right="2rem", bottom="1rem", transition=0)

# Float button container
button_container.float(button_css)

# Add Float Box with embedded Youtube video
float_box('<iframe width="100%" height="100%" src="https://www.youtube.com/embed/IohuG0z_4Wc" title="#Saama4Me – Come join us!" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>',width="29rem", right="2rem", bottom=vid_y_pos, css="padding: 0;transition-property: all;transition-duration: .5s;transition-timing-function: cubic-bezier(0, 1, 0.5, 1);", shadow=12)


# with st.popover("Open popover"):
#     st.markdown("Hello World 👋")
#     name = st.text_input("What's your name?")
#     address = st.text_input("What's your Address?")
# st.write("Your name:", name)
# st.write("Your Address:", address)
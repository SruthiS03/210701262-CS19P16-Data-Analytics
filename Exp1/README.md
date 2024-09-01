# Hadoop-Installation
# Apache Hadoop Installation Manual for v3.4.0

## Reqirements
### Java
- Hadoop requires Java version `8`
- If you have Java installed already, Uninstall it from `Control Panel` --> `Uninstall a program`
- Java version 8 can be downloaded from: [JavaJDKv8](https://www.oracle.com/in/java/technologies/javase/javase8u211-later-archive-downloads.html)
- Download `Windows x86 Installer` if you have Intel processor, otherwise download `Windows x64 Installer`
- Install the package

### 7Zip
- Hadoop comes in compressed tarballs (.tar.gz), 7Zip is a great tool to decompress them.
- 7Zip can be downloaded from [download](https://www.7-zip.org/)
- Install 7Zip

### Hadoop
- Download hadoop from officical release page: [link](https://hadoop.apache.org/releases.html)
- download the `binary` without fail.

## Installation
### Decompression
- Open 7Zip as `Administrator`
- Unzip the downloaded hadoop-<VER>.tar.gz.
- This will produce a tarball (.tar) file and unzip tarball again.
- Copy the Uzipped folder to C:\

### Decompression (Alternate Way)
- run `mkdir C:\Hadoop` in cmd
- run `tar -xvzf  hadoop-3.3.0.tar.gz -C C:\Hadoop\` from the Downloads directory.

### Setting up the Environment
- Search for `Edit the System Environment Vairables` --> then click `Environment Variables`
- Under `System variables` click `New` and name it `JAVA_HOME` and value as jdk installation directory `C:\Java` (may vary based on where you install java)
- Again click `New` and name it `HADOOP_HOME` and value as hadoop installation directory `C:\Hadoop\hadoop-<VER>` (may vary based on where you extracted hadoop)
- You can verify if the Environment Variables are set by `echo %VARIABLE_NAME%` in cmd.
- Click on `Path` variable and click `Edit`
- Add the following to Path variables one by one
```
%JAVA_HOME%/bin
%HADOOP_HOME%/bin
```
- verify this by `java -version` and `hadoop -version` in command prompt.

### Additional Packages
- Download the `bin.zip` from this repository.
- Extract it and paste it in `%HADOOP_HOME%\bin`
- Open and run `winutils.exe` from `%HADOOP_HOME%\bin`

## Configuration

Now we are ready to configure the most important part - Hadoop configurations which involves Core, YARN, MapReduce, HDFS configurations. 

### Configure core site

Edit file **core-site.xml** in %HADOOP_HOME%\etc\hadoop folder. 

For my environment, the actual path is C:\Hadoop\hadoop-3.4.0\etc\hadoop

Replace configuration element with the following:

```xml
<configuration>
   <property>
     <name>fs.default.name</name>
     <value>hdfs://0.0.0.0:19000</value>
   </property>
</configuration>
```



### Configure HDFS

Edit file **hdfs-site.xml** in %HADOOP_HOME%\etc\hadoop folder. 

Before editing, please correct two folders in your system: one for namenode directory and another for data directory.  For my system, I created the following two sub folders:

```
mkdir C:\hadoop\hadoop-3.4.0\data\datanode
```

```
mkdir C:\hadoop\hadoop-3.4.0\data\namenode
```


Replace configuration element with the following (remember to replace the highlighted paths accordingly):

```xml
<configuration>
   <property>
     <name>dfs.replication</name>
     <value>1</value>
   </property>
   <property>
     <name>dfs.namenode.name.dir</name>
     <value>/hadoop/hadoop-3.4.0/data/namenode</value>
   </property>
   <property>
     <name>dfs.datanode.data.dir</name>
     <value>/hadoop/hadoop-3.4.0/data/datanode</value>
   </property>
</configuration>
```



### Configure MapReduce and YARN site

Edit file **mapred-site.xml** in %HADOOP_HOME%\etc\hadoop folder. 

Replace configuration element with the following:

```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property> 
        <name>mapreduce.application.classpath</name>
        <value>%HADOOP_HOME%/share/hadoop/mapreduce/*,%HADOOP_HOME%/share/hadoop/mapreduce/lib/*,%HADOOP_HOME%/share/hadoop/common/*,%HADOOP_HOME%/share/hadoop/common/lib/*,%HADOOP_HOME%/share/hadoop/yarn/*,%HADOOP_HOME%/share/hadoop/yarn/lib/*,%HADOOP_HOME%/share/hadoop/hdfs/*,%HADOOP_HOME%/share/hadoop/hdfs/lib/*</value>
    </property>
</configuration>
```

Edit file **yarn-site.xml** in %HADOOP_HOME%\etc\hadoop folder. 

```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME,HADOOP_HOME</value>
    </property>
</configuration>
```

## Initialization
### HDFS 

Run the following command in Command Prompt 

```
hdfs namenode -format
```

### Start HDFS daemons 

Run the following command to start HDFS daemons in Command Prompt:

```
%HADOOP_HOME%\sbin\start-dfs.cmd
```

Two Command Prompt windows will open: one for datanode and another for namenode **DONOT CLOSE THESE WINDOWS**

Verify HDFS Web portal by opening `http://localhost:9870/dfshealth.html#tab-overview` in your browser.

### Start YARN daemons

warning You may encounter permission issues if you start YARN daemons using normal user. To ensure you don't encounter any issues. Please open a Command Prompt window using Run as administrator.

Run the following command in an elevated Command Prompt window (Run as administrator) to start YARN daemons:

```
%HADOOP_HOME%\sbin\start-yarn.cmd
```

Similarly two Command Prompt windows will open: one for resource manager and another for node manager **DONOT CLOSE THESE WINDOWS**
- Verify Hadoop Resource manager by opening `http://localhost:8088` in you browser.
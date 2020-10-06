# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

PATH=$PATH:$HOME/bin
export PATH
export http_proxy=http://192.168.22.118:3128
export ftp_proxy=http://192.168.22.118:3128
export all_proxy=http://192.168.22.118:3128
export https_proxy=http://192.168.22.118:3128
export no_http_proxy=localhost|127.0.0.1

JAVA_HOME="/home/xia/jdk1.8.0_191"
JRE_HOME=$JAVA_HOME/jre
CLASSPATH=:$JAVA_HOME/lib:$JRE_HOME/lib
export JAVA_HOME JRE_HOME PATH CLASSPATH
export PATH=$JAVA_HOME/bin:$PATH

export MAVEN_HOME="/home/xia/apache-maven-3.5.2"
export MAVEN_OPTS="-Xms512m -Xmx1024m"
export PATH=$MAVEN_HOME/bin:$PATH










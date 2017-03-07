function get() {
    INIFILE=$1; SECTION=$2; ITEM=$3
    value=`sed -nr "/^\[$2\]/ { :l /^$3[ ]*=/ { s/.*=[ ]*//; p; q;}; n; b l;}" $1`
    echo ${value}
}

cat url100M.bat | sort | uniq -c | sort -k1,1nr | head -100|awk -F ' ' '{print $1 "\t" $2}'|sort > diff1
cat top100.bat|awk -F '\t' '{print $2 "\t" $1}'|sort > diff2
awk -F '\t' 'BEGIN{
    line[1] = 0
    url[1] = 0
    count1 = 0
    count2 = 0
}
FILENAME ~ "diff1"{
    line["line"count1]=$1
    url["line"count1]=$2
    count1+=1
}
FILENAME ~ "diff2"{
    t = line["line"count2]
    if(t!=$1){
       print "diff1" t "diff2" $1
       print "diff1" url["line"count2] "diff2" $2
    }
    count2+=1
}   
' diff1 diff2
#diff diff1 diff2
#include<iostream>
#include<fstream>
#include<time.h>
using namespace std;
const int UrlCount = (1024*8)*1024/8;//产生大约2G
const int UrlLen = 128 -  7;//128 - http://
string generate_url(int m, int n){
    string res = "http://";
    for(int i = 0; i < m; ++i){
        int t = rand() % 10;
        res.push_back('0' + t);
    }
    for(int i = 0; i < n; ++i){
        int t = rand() % 26;
        res.push_back('a' + t);
    }
    return res;
}
int main()
{
    srand((unsigned)time(NULL));
    ofstream out_file("url100M.bat");
    for(int i = 0; i < UrlCount; ++i){
         int num = rand() % UrlLen;
         int char_num = rand() % UrlLen;
         if(num + char_num >= UrlLen)
            num = UrlLen - char_num - 1;
        string res = generate_url(num, char_num);
        out_file<<res<<endl;
    }
    out_file.close();
}
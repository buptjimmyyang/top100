#include<iostream>
#include<string>
#include<map>
#include<fstream>
#include<thread>
#include<mutex>
#include<condition_variable>
#include<functional>
#include<vector>
#include<queue>
#include<sys/time.h>
using namespace std;
/**
 * 总体思路：
 *
 * 1. 总的任务数目：1个读线程+n个写线程+1合并线程
 * 2. 读线程每隔500M进行统计计数结果通过输出线程输出到part*，
 * 写任务全部结束后进行多路归并，合并url相同的元素
 * 3. 将合并完成的url通过大小为100的小根堆统计top100
 * **/
const int ReadLine = (8*1024)*512;//每次读取不超过500M 限制每行最多128个字符串
int finishWriteTask = 0; //已经完成的写任务数目
int initWriteTask = 0;  //写任务数目
const int TOP100 = 100;
mutex writeLock;
condition_variable writeFinishCond;//所有文件写完成后开始合并操作

//封装map
class StatMap{
public:
    bool isValid;
    map<string, int> urlCount;
    condition_variable cond;
    mutex mut;
    StatMap(): isValid(true)
    {
        
    }

    void waitLock(){
        if(!isValid){
           unique_lock<std::mutex> lck(mut);
           cond.wait(lck);
        }
    }

    void notifyLock(){
        unique_lock<std::mutex> lck(mut);
        isValid = true;
        cond.notify_one();
    }
};
//双缓冲机制使写线程异步写读线程不使用的map
StatMap statMap[2];

class WriteTask{
    const int fileId;
    StatMap *curMap;
    thread *task;
public:
    WriteTask(int fileId, StatMap *curMap):fileId(fileId),curMap(curMap){
        task =  new thread(&WriteTask::writeFile, this);
    }

    void writeFile(){
        string fileName = "part";
        fileName += to_string(fileId);
        ofstream outPut(fileName);
        for(map<string, int>::iterator s = curMap->urlCount.begin(); s != curMap->urlCount.end(); ++s){
            outPut<<s->first<<'\t'<<s->second<<endl;
        }

        outPut.close();
        curMap->urlCount.clear();
        curMap->notifyLock();

        unique_lock<mutex> lck(writeLock);
        if(initWriteTask > finishWriteTask){
            finishWriteTask += 1;
        }
        else
            writeFinishCond.notify_one();
    }
    
    ~WriteTask(){
         task->join();
    }
};

// 多路归并记录url和数目以及对于的文件id
class UrlElem{
    public:
        string url;
        int count;
        int fileId;
    
    UrlElem(): url(""), count(0), fileId(0){

    }
};

class ReadFileOne{
    ifstream inPut;
    int fileId;
    public:
        ReadFileOne(int fileId): fileId(fileId){
            string fileName = string("part") + to_string(fileId);
            inPut.open(fileName);
        }

        UrlElem getOneElem(){
            UrlElem ret;
            if(inPut.eof())
                return ret;
            ret.fileId = fileId;
            inPut>>ret.url>>ret.count;
            return ret;
        }

};

class MergeTask{
    struct CmpCount{
        bool operator()(const UrlElem &u1, const UrlElem &u2){
            return u1.count > u2.count;
        }
    };

    struct CmpUrl{
        bool operator()(const UrlElem &u1, const UrlElem &u2){
            return u1.url > u2.url;
        }
    };

    vector<ReadFileOne> files;
    priority_queue<UrlElem, vector<UrlElem>, CmpCount> top100;
    priority_queue<UrlElem, vector<UrlElem>, CmpUrl> mergePart;
    int mergeFinishCount;
public:
    MergeTask(): mergeFinishCount(0){
        for(int i = 0; i < initWriteTask; ++i){
            files.push_back(ReadFileOne(i));
            UrlElem ret = files[i].getOneElem();
            mergePart.push(ret);
        }
    }

    void start(){
        UrlElem lastElem;
        UrlElem curElem;

        while(mergeFinishCount < initWriteTask){
            curElem = mergePart.top();
            mergePart.pop();
        
            if(lastElem.url == curElem.url)
                lastElem.count += curElem.count;
            else{
                makeTop100(lastElem);
                lastElem = curElem;
            }
            curElem = files[curElem.fileId].getOneElem();
            if(curElem.url == ""){
                mergeFinishCount += 1;
            }
            else{
                mergePart.push(curElem);
            }
        }
        makeTop100(lastElem);
        while(mergePart.size()){
            lastElem = mergePart.top();
            mergePart.pop();
            makeTop100(lastElem);
        }
        writeTop100();
    }

     void makeTop100(const UrlElem &lastElem){
            if(lastElem.url == "")
                return;
            if(top100.size() < TOP100){
                top100.push(lastElem);
                return;
            }
            UrlElem top = top100.top();
            if(top.count >= lastElem.count)
                return;
            top100.pop();
            top100.push(lastElem);
        }

        void writeTop100(){
            ofstream outPut("top100.bat");
            while(top100.size()){
                UrlElem ret = top100.top();
                top100.pop();
                outPut<<ret.url<<"\t"<<ret.count<<endl;
            }
            outPut.close();
        }
};

class ReadTask{
    private:
        int finishReadLine;
        int curStat;
    public:
        ReadTask(): curStat(0), finishReadLine(0){

        }
        
        void readFile(const string &fileName){
            struct timeval pre[2];
            gettimeofday(&pre[0], NULL);
            FILE *fp=fopen(fileName.c_str(), "r");
            char url[136]; //足够容纳最大长度的一行！
            StatMap *curMap = &statMap[curStat];
            while(fgets(url, sizeof(url), fp)){
                curMap->urlCount[string(url)] += 1;
                ++finishReadLine;
                if(finishReadLine >= ReadLine){
                    startWrite(curMap);
                }
            }
            startWrite(curMap);
            //判断是否写完文件
            unique_lock<std::mutex> lck(writeLock);
            if(finishWriteTask < initWriteTask){
                  writeFinishCond.wait(lck);
            }
            gettimeofday(&pre[1], NULL);
            cout<<"read and write time"<<((long long)pre[1].tv_sec - pre[0].tv_sec) * 1000000 + pre[1].tv_usec - pre[0].tv_usec<<endl;

            //开始合并
            struct timeval tv[2];
            gettimeofday(&tv[0], NULL);
            MergeTask merge;
            merge.start();
            gettimeofday(&tv[1], NULL);
            cout<<"merge time"<<((long long)tv[1].tv_sec - tv[0].tv_sec) * 1000000 + tv[1].tv_usec - tv[0].tv_usec<<endl;
            fclose(fp);
        }

        void startWrite(StatMap *&curMap){
            //开启写线程
            WriteTask writeThread(initWriteTask, curMap);
            initWriteTask += 1;
            finishReadLine = 0;
            curStat = (curStat + 1) % 2;
            curMap = &statMap[curStat];
            curMap->waitLock();
        }
    

};
int main(){
    struct timeval tv[2];
    gettimeofday(&tv[0], NULL);
    ReadTask task;
    task.readFile("url100M.bat");
    gettimeofday(&tv[1], NULL);
    cout<<"total time"<<((long long)tv[1].tv_sec - tv[0].tv_sec) * 1000000 + tv[1].tv_usec - tv[0].tv_usec<<endl;

}
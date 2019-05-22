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
#include<chrono>
using namespace std;
/**
 * 总体思路：
 *
 * 1. 总的任务数目：1个读线程+n个写线程+1合并线程
 * 2. 读线程每隔500M进行统计计数结果通过输出线程输出到part*，
 * 写任务全部结束后进行多路归并，合并url相同的元素
 * 3. 将合并完成的url通过大小为100的小根堆统计top100
 * **/
const int ReadLine = (8*1024)*512/10;//每次读取不超过500M 限制每行最多128个字符串
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
        curMap->isValid = false;
        task =  new thread(&WriteTask::writeFile, this);
    }

    void writeFile(){
        string fileName = "part";
        fileName += to_string(fileId);
        ofstream outPut(fileName);
        for(map<string, int>::iterator s = curMap->urlCount.begin(); s != curMap->urlCount.end(); ++s){
            outPut<<s->first<<'\t'<<s->second<<endl;
        }

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
        //因为各个part已经按照url顺序存储，merge阶段首先将各个part的首个url放入优先队列中，取最小url并在该
        //队列中再取下一个元素放入优先队列，若下次url与上次相同则合并url数目，否则上次的url计数统计结束 放入top100
        //堆中，进行计数
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
            auto start_time = chrono::system_clock::now();
            ifstream inPut(fileName);
            StatMap *curMap = &statMap[curStat];
            string url;
            while(!inPut.eof()){
                inPut>>url;
                curMap->urlCount[url] += 1;
                ++finishReadLine;
                if(finishReadLine >= ReadLine){
                    startWrite(curMap);
                }
                url.clear();
            }
            startWrite(curMap);
            //判断是否写完文件
            unique_lock<std::mutex> lck(writeLock);
            if(finishWriteTask < initWriteTask){
                  writeFinishCond.wait(lck);
            }
            auto end_time = chrono::system_clock::now();
            auto duration = chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            cout<<"read and write time"<<duration.count()<<endl;

            //开始合并
            start_time = chrono::system_clock::now();
            MergeTask merge;
            merge.start();
            end_time = chrono::system_clock::now();
            duration = chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            cout<<"merge time"<<duration.count()<<endl;
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
    auto start_time = chrono::system_clock::now();

    ReadTask task;
    task.readFile("url.bat");

    auto end_time = chrono::system_clock::now();
    auto duration = chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    cout<<"total time"<<duration.count()<<endl;
}
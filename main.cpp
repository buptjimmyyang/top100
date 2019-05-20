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
 * ����˼·��
 *
 * 1. �ܵ�������Ŀ��1�����߳�+n��д�߳�+1�ϲ��߳�
 * 2. ���߳�ÿ��500M����ͳ�Ƽ������ͨ������߳������part*��
 * д����ȫ����������ж�·�鲢���ϲ�url��ͬ��Ԫ��
 * 3. ���ϲ���ɵ�urlͨ����СΪ100��С����ͳ��top100
 * **/
const int ReadLine = (8*1024)*512;//ÿ�ζ�ȡ������500M ����ÿ�����128���ַ���
int finishWriteTask = 0; //�Ѿ���ɵ�д������Ŀ
int initWriteTask = 0;  //д������Ŀ
const int TOP100 = 100;
mutex writeLock;
condition_variable writeFinishCond;//�����ļ�д��ɺ�ʼ�ϲ�����

//��װmap
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
//˫�������ʹд�߳��첽д���̲߳�ʹ�õ�map
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

// ��·�鲢��¼url����Ŀ�Լ����ڵ��ļ�id
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
            char url[136]; //�㹻������󳤶ȵ�һ�У�
            StatMap *curMap = &statMap[curStat];
            while(fgets(url, sizeof(url), fp)){
                curMap->urlCount[string(url)] += 1;
                ++finishReadLine;
                if(finishReadLine >= ReadLine){
                    startWrite(curMap);
                }
            }
            startWrite(curMap);
            //�ж��Ƿ�д���ļ�
            unique_lock<std::mutex> lck(writeLock);
            if(finishWriteTask < initWriteTask){
                  writeFinishCond.wait(lck);
            }
            gettimeofday(&pre[1], NULL);
            cout<<"read and write time"<<((long long)pre[1].tv_sec - pre[0].tv_sec) * 1000000 + pre[1].tv_usec - pre[0].tv_usec<<endl;

            //��ʼ�ϲ�
            struct timeval tv[2];
            gettimeofday(&tv[0], NULL);
            MergeTask merge;
            merge.start();
            gettimeofday(&tv[1], NULL);
            cout<<"merge time"<<((long long)tv[1].tv_sec - tv[0].tv_sec) * 1000000 + tv[1].tv_usec - tv[0].tv_usec<<endl;
            fclose(fp);
        }

        void startWrite(StatMap *&curMap){
            //����д�߳�
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
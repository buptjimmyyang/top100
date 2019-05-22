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
 * ����˼·��
 *
 * 1. �ܵ�������Ŀ��1�����߳�+n��д�߳�+1�ϲ��߳�
 * 2. ���߳�ÿ��500M����ͳ�Ƽ������ͨ������߳������part*��
 * д����ȫ����������ж�·�鲢���ϲ�url��ͬ��Ԫ��
 * 3. ���ϲ���ɵ�urlͨ����СΪ100��С����ͳ��top100
 * **/
const int ReadLine = (8*1024)*512/10;//ÿ�ζ�ȡ������500M ����ÿ�����128���ַ���
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
        //��Ϊ����part�Ѿ�����url˳��洢��merge�׶����Ƚ�����part���׸�url�������ȶ����У�ȡ��Сurl���ڸ�
        //��������ȡ��һ��Ԫ�ط������ȶ��У����´�url���ϴ���ͬ��ϲ�url��Ŀ�������ϴε�url����ͳ�ƽ��� ����top100
        //���У����м���
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
            //�ж��Ƿ�д���ļ�
            unique_lock<std::mutex> lck(writeLock);
            if(finishWriteTask < initWriteTask){
                  writeFinishCond.wait(lck);
            }
            auto end_time = chrono::system_clock::now();
            auto duration = chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            cout<<"read and write time"<<duration.count()<<endl;

            //��ʼ�ϲ�
            start_time = chrono::system_clock::now();
            MergeTask merge;
            merge.start();
            end_time = chrono::system_clock::now();
            duration = chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            cout<<"merge time"<<duration.count()<<endl;
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
    auto start_time = chrono::system_clock::now();

    ReadTask task;
    task.readFile("url.bat");

    auto end_time = chrono::system_clock::now();
    auto duration = chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    cout<<"total time"<<duration.count()<<endl;
}
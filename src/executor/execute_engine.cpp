#include "executor/execute_engine.h"
#include <time.h>
#include <algorithm>
#include <vector>
#include "glog/logging.h"
#include "parser/minisql_lex.h"
ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  DBStorageEngine* db = new DBStorageEngine(ast->child_->val_);
  //����mapӳ������
  dbs_[ast->child_->val_]=db;
  //cout<<"ExecuteCreateDatabase SUCCESS!"<<endl;
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  delete dbs_[ast->child_->val_];
  dbs_.erase(ast->child_->val_);
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  cout<<"------DataBases------"<<endl;
  for (auto p=dbs_.begin();p!=dbs_.end();p++){
    std::cout<<p->first<<std::endl;
  }
  //cout<<"ExecuteShowDatabases SUCCESS!"<<endl;
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif

  if(dbs_.count(ast->child_->val_)){
    current_db_=ast->child_->val_;
//    DBStorageEngine* current_db=dbs_.find(current_db_)->second;
//    current_db=dbs_[current_db_];
  }else{
    cout<<"Invalid DataBase Name!"<<endl;
  }
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  cout<<"------Tables------"<<endl;
  vector<TableInfo* > tables;
  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  current_db->catalog_mgr_->GetTables(tables);
  for(auto p=tables.begin();p<tables.end();p++){
    cout<<(*p)->GetTableName()<<endl;
  }
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  string table_name = ast->child_->val_;
  //cout<<"table_name:"<<table_name<<endl;
  pSyntaxNode column_pointer= ast->child_->next_->child_;//��һ�����Զ�Ӧ��ָ��
  vector<Column*>vec_col;
  while(column_pointer!=nullptr&&column_pointer->type_==kNodeColumnDefinition){
    //���ĺ��� �Ǳ���������Ϣ
    //cout<<"---------------"<<endl;
    bool is_unique = false;
    if (column_pointer->val_!=nullptr){
      string s = column_pointer->val_;
      is_unique = (s=="unique");
    }
    string column_name = column_pointer->child_->val_;//��������
    //cout<<"column_name:"<<column_name<<endl;
    string column_type = column_pointer->child_->next_->val_;//��������
    //cout<<"column_type:"<<column_type<<endl;
    int cnt = 0;
    Column *now;
    if(column_type=="int"){
      now = new Column(column_name,kTypeInt,cnt,true,is_unique);
    }
    else if(column_type=="char"){
      string len = column_pointer->child_->next_->child_->val_;
      //cout<<"len:"<<len<<endl;
      long unsigned int a = -1;
      if (len.find('.')!=a){
        cout<<"Semantic Error, String Length Can't be a Decimal!"<<endl;
        return DB_FAILED;
      }
      int length = atoi(column_pointer->child_->next_->child_->val_);
      if (length<0){
        cout<<"Semantic Error, String Length Can't be Negative!"<<endl;
        return DB_FAILED;
      }
      now = new Column(column_name,kTypeChar,length,cnt,true,is_unique);
    }
    else if(column_type=="float"){
      now = new Column(column_name,kTypeFloat,cnt,true,is_unique);
    }
    else{
      cout<<"Error Column Type!"<<endl;
      return DB_FAILED;
    }
    //cout<<"is_unique:"<<is_unique<<endl;
    vec_col.push_back(now);
    column_pointer = column_pointer->next_;
    cnt++;
  }

  Schema *schema = new Schema(vec_col);
  TableInfo *table_info = nullptr;

  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  dberr_t IsCreate=current_db->catalog_mgr_->CreateTable(table_name,schema,nullptr,table_info);
  if(IsCreate==DB_TABLE_ALREADY_EXIST){
    cout<<"Table Already Exist!"<<endl;
    return IsCreate;
  }
  if (column_pointer!=nullptr){
    //cout<<"It has primary key!"<<endl;
    pSyntaxNode key_pointer = column_pointer->child_;
    vector <string>primary_keys;
    while(key_pointer!=nullptr){
      string key_name = key_pointer->val_ ;
      //cout<<"key_name:"<<key_name<<endl;
      primary_keys.push_back(key_name);
      key_pointer = key_pointer->next_;
    }
    CatalogManager* current_catalog=current_db->catalog_mgr_;
    IndexInfo* indexinfo=nullptr;
    string index_name = table_name + "_pk";

    current_catalog->CreateIndex(table_name,index_name,primary_keys,nullptr,indexinfo);
  }

  for (auto & r : vec_col){
    if (r->is_unique()){
      string unique_index_name = table_name + "_"+r->GetName()+"_unique";
      CatalogManager* current_catalog=current_db->catalog_mgr_;
      vector <string>unique_attribute_name = {r->GetName()};
      IndexInfo* indexinfo=nullptr;
      current_catalog->CreateIndex(table_name,unique_index_name,unique_attribute_name,nullptr,indexinfo);
    }
  }
  return IsCreate;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  dberr_t IsDrop=current_db->catalog_mgr_->DropTable(ast->child_->val_);
  if(IsDrop==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
  }
  return IsDrop;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  cout<<"------Indexes------"<<endl;
  vector<TableInfo* > tables;
  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  current_db->catalog_mgr_->GetTables(tables);

  for(auto p=tables.begin();p<tables.end();p++){

    cout<<"Indexes of Table "<<(*p)->GetTableName()<<":"<<endl;
    vector<IndexInfo*> indexes;
    current_db->catalog_mgr_->GetTableIndexes((*p)->GetTableName(),indexes);
    for(auto q=indexes.begin();q<indexes.end();q++){
      cout<<(*q)->GetIndexName()<<endl;
    }
  }
  return DB_SUCCESS;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  string table_name = ast->child_->next_->val_;
  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  CatalogManager* current_catalog=current_db->catalog_mgr_;
  TableInfo *tableinfo = nullptr;
  current_catalog->GetTable(table_name, tableinfo);

  pSyntaxNode key_name=ast->child_->next_->next_->child_;
  for(;key_name!=nullptr;key_name=key_name->next_){
    uint32_t key_index;
    dberr_t IsIn = tableinfo->GetSchema()->GetColumnIndex(key_name->val_,key_index);
    if (IsIn==DB_COLUMN_NAME_NOT_EXIST){
      cout<<"Attribute "<<key_name->val_<<" Isn't in The Table!"<<endl;
      return DB_FAILED;
    }
    const Column* ky=tableinfo->GetSchema()->GetColumn(key_index);
    if(ky->is_unique()==false){
      cout<<"Can't Create Index On Non-unique Key!"<<endl;
      return DB_FAILED;
    }
  }
  vector <string> index_keys;
  //�õ�index_key�ĵ�һ�����???
  pSyntaxNode index_key=ast->child_->next_->next_->child_;
  for(;index_key!=nullptr;index_key=index_key->next_){
    index_keys.push_back(index_key->val_);
  }
  IndexInfo* indexinfo=nullptr;
  string index_name = ast->child_->val_;
  dberr_t IsCreate=current_catalog->CreateIndex(table_name,index_name,index_keys,nullptr,indexinfo);
  if(IsCreate==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
  }
  if(IsCreate==DB_INDEX_ALREADY_EXIST){
    cout<<"Index Already Exist!"<<endl;
  }

  TableHeap* tableheap = tableinfo->GetTableHeap();
  vector<uint32_t>index_column_number;
  for (auto r = index_keys.begin(); r != index_keys.end() ; r++ ){
    uint32_t index ;
    tableinfo->GetSchema()->GetColumnIndex(*r,index);
    index_column_number.push_back(index);
  }
  vector<Field>fields;
  for (auto iter=tableheap->Begin(nullptr) ; iter!= tableheap->End(); iter++) {
    Row& it_row = *iter;
    vector<Field> index_fields;
    for (unsigned int & m : index_column_number){
      index_fields.push_back(*(it_row.GetField(m)));
    }
    Row index_row(index_fields);
    indexinfo->GetIndex()->InsertEntry(index_row,it_row.GetRowId(),nullptr);
  }
  return IsCreate;
  //return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif

  vector<TableInfo* > tables;
  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  current_db->catalog_mgr_->GetTables(tables);

  for(auto p=tables.begin();p<tables.end();p++){

    vector<IndexInfo*> indexes;
    current_db->catalog_mgr_->GetTableIndexes((*p)->GetTableName(),indexes);
    string index_name=ast->child_->val_;
    for(auto q=indexes.begin();q<indexes.end();q++){

      if((*q)->GetIndexName()==index_name){
        dberr_t IsDrop=current_db->catalog_mgr_->DropIndex((*p)->GetTableName(),index_name);
        if(IsDrop==DB_TABLE_NOT_EXIST){
          cout<<"Table Not Exist!"<<endl;
        }
        if(IsDrop==DB_INDEX_NOT_FOUND){
          cout<<"Index Not Found!"<<endl;
        }
        return IsDrop;
      }
    }
  }

  cout<<"Index Not Found!"<<endl;
  return DB_FAILED;
}

vector<Row*> rec_sel(pSyntaxNode sn, std::vector<Row*>& r, TableInfo* t, CatalogManager* c){
  if(sn == nullptr) return r;
  if(sn->type_ == kNodeConnector){

    vector<Row*> ans;
    if(strcmp(sn->val_,"and") == 0){
      auto r1 = rec_sel(sn->child_,r,t,c);
      ans = rec_sel(sn->child_->next_,r1,t,c);
      return ans;
    }
    else if(strcmp(sn->val_,"or") == 0){
      auto r1 = rec_sel(sn->child_,r,t,c);
      auto r2 = rec_sel(sn->child_->next_,r,t,c);
      for(uint32_t i=0;i<r1.size();i++){
        ans.push_back(r1[i]);
      }
      for(uint32_t i=0;i<r2.size();i++){
        int flag=1;//û���ظ�
        for(uint32_t j=0;j<r1.size();j++){
          int f=1;
          for(uint32_t k=0;k<r1[i]->GetFieldCount();k++){
            if(!r1[i]->GetField(k)->CompareEquals(*r2[j]->GetField(k))){
              f=0;break;
            }
          }
          if(f==1){
            flag=0;//���ظ�
            break;}
        }
        if(flag==1) ans.push_back(r2[i]);
      }
      return ans;
    }
  }
  if(sn->type_ == kNodeCompareOperator){
    string op = sn->val_;//operation type
    string col_name = sn->child_->val_;//column name
    string val = sn->child_->next_->val_;//compare value
    uint32_t keymap;
    vector<Row*> ans;
    if(t->GetSchema()->GetColumnIndex(col_name, keymap)!=DB_SUCCESS){
      cout<<"column not found"<<endl;
      return ans;
    }
    const Column* key_col = t->GetSchema()->GetColumn(keymap);
    TypeId type =  key_col->GetType();

    if(op == "="){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);

        vector <IndexInfo*> indexes;
        c->GetTableIndexes(t->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
          if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
            if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
              // cout<<"--select using index--"<<endl;
              Row tmp_row(vect_benchmk);
              vector<RowId> result;
              (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
              for(auto q:result){
                if(q.GetPageId()<0) continue;
                Row *tr = new Row(q);
                t->GetTableHeap()->GetTuple(tr,nullptr);
                ans.push_back(tr);
              }
              return ans;
            }
          }
        }
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }

      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }

      else if(type==kTypeChar){
        char *ch = new char[val.size()];
        strcpy(ch,val.c_str());//input compare object
        // cout<<"ch "<<sizeof(ch)<<endl;
        Field benchmk = Field(TypeId::kTypeChar, const_cast<char *>(ch), val.size(), true);
        // Field benchmk(kTypeChar,ch,key_col->GetLength(),true);
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);

        vector <IndexInfo*> indexes;
        c->GetTableIndexes(t->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
          if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
            if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){

              // cout<<"--select using index--"<<endl;
              clock_t s,e;
              s=clock();
              Row tmp_row(vect_benchmk);
              vector<RowId> result;
              (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
              e=clock();
              cout<<"name index select takes "<<double(e-s)/CLOCKS_PER_SEC<<"s to Execute."<<endl;
              for(auto q:result){
                if(q.GetPageId()<0) continue;
                // cout<<"index found"<<endl;
                Row *tr = new Row(q);
                t->GetTableHeap()->GetTuple(tr,nullptr);
                ans.push_back(tr);
              }
              return ans;
            }
          }
        }

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)==0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<"){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()+2];
        strcpy(ch,val.c_str());
        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();
          if(strcmp(test,ch)<0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">"){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThan(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)>0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<="){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareLessThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)<=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == ">="){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareGreaterThanEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)>=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    else if(op == "<>"){
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeFloat)
      {
        float valfloat = std::stof(val);
        Field benchmk(type,float(valfloat));
        for(uint32_t i=0;i<r.size();i++){
          if(!r[i]->GetField(keymap)->CheckComparable(benchmk)){
            cout<<"not comparable"<<endl;
            return ans;
          }
          if(r[i]->GetField(keymap)->CompareNotEquals(benchmk)){
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
      else if(type==kTypeChar){
        char* ch = new char[key_col->GetLength()];
        strcpy(ch,val.c_str());//�Ƚ�Ŀ�����ch��

        for(uint32_t i=0;i<r.size();i++){
          const char* test = r[i]->GetField(keymap)->GetData();

          if(strcmp(test,ch)!=0){
            vector<Field> f;
            for(auto it:r[i]->GetFields()){
              f.push_back(*it);
            }
            Row* tr = new Row(*r[i]);
            ans.push_back(tr);
          }
        }
      }
    }
    return ans;
  }
  return r;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  pSyntaxNode range = ast->child_;
  vector<uint32_t> columns;
  string table_name=range->next_->val_;
  TableInfo *tableinfo = nullptr;
  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
    return DB_FAILED;
  }
  if(range->type_ == kNodeAllColumns){
    // cout<<"select all"<<endl;
    for(uint32_t i=0;i<tableinfo->GetSchema()->GetColumnCount();i++)
      columns.push_back(i);
  }
  else if(range->type_ == kNodeColumnList){
    // vector<Column*> all_columns = tableinfo->GetSchema()->GetColumns();
    pSyntaxNode col = range->child_;
    while(col!=nullptr){
      uint32_t pos;
      if(tableinfo->GetSchema()->GetColumnIndex(col->val_,pos)==DB_SUCCESS){
        columns.push_back(pos);
      }
      else{
        cout<<"column not found"<<endl;
        return DB_FAILED;
      }
      col = col->next_;
    }
  }
  cout<<"--------------------"<<endl;
  //cout<<endl;
  for(auto i:columns){
    cout<<tableinfo->GetSchema()->GetColumn(i)->GetName()<<"   ";
  }
  cout<<endl;
  cout<<"--------------------"<<endl;
  if(range->next_->next_==nullptr)//û��ѡ������
  {
    int cnt=0;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      for(uint32_t j=0;j<columns.size();j++){
        if(it->GetField(columns[j])->IsNull()){
          cout<<"null";
        }
        else
          it->GetField(columns[j])->print();
        cout<<"  ";

      }
      cout<<endl;
      cnt++;
    }
    cout<<"Select Success, Affects "<<cnt<<" Record!"<<endl;
    return DB_SUCCESS;
  }
  else if(range->next_->next_->type_ == kNodeConditions){
    pSyntaxNode cond = range->next_->next_->child_;
    vector<Row*> origin_rows;
    string op = cond->val_;
    if(cond->type_ == kNodeCompareOperator && op == "="){
      string col_name = cond->child_->val_;//column name
      string val = cond->child_->next_->val_;//compare value
      uint32_t keymap;
      if(tableinfo->GetSchema()->GetColumnIndex(col_name, keymap)!=DB_SUCCESS){
        cout<<"column not found"<<endl;
        return DB_SUCCESS;
      }
      const Column* key_col = tableinfo->GetSchema()->GetColumn(keymap);
      TypeId type =  key_col->GetType();
      if(type==kTypeInt)
      {
        int valint = std::stoi(val);
        Field benchmk(type,int(valint));
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);
        vector <IndexInfo*> indexes;

        current_db->catalog_mgr_->GetTableIndexes(tableinfo->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
          if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
            if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
              cout<<"--select int using index--"<<endl;
              Row tmp_row(vect_benchmk);
              vector<RowId> result;
              (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
              for(auto q:result){
                if(q.GetPageId()<0) continue;
                Row *tr = new Row(q);
                tableinfo->GetTableHeap()->GetTuple(tr,nullptr);
                for(uint32_t j=0;j<columns.size();j++){
                  tr->GetField(columns[j])->print();
                  cout<<"  ";
                }
                cout<<endl;
              }
              return DB_SUCCESS;
            }
          }
        }
      }
      else if(type==kTypeChar){
        char *ch = new char[val.size()];
        strcpy(ch,val.c_str());//input compare object
        Field benchmk = Field(TypeId::kTypeChar, const_cast<char *>(ch), val.size(), true);
        vector<Field> vect_benchmk;
        vect_benchmk.push_back(benchmk);
        vector <IndexInfo*> indexes;

        current_db->catalog_mgr_->GetTableIndexes(tableinfo->GetTableName(),indexes);
        for(auto p=indexes.begin();p<indexes.end();p++){
          if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
            if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col_name){
              cout<<"--select using char* index--"<<endl;
              Row tmp_row(vect_benchmk);
              vector<RowId> result;
              (*p)->GetIndex()->ScanKey(tmp_row,result,nullptr);
              for(auto q:result){
                if(q.GetPageId()<0) continue;
                Row *tr = new Row(q);
                tableinfo->GetTableHeap()->GetTuple(tr,nullptr);
                for(uint32_t j=0;j<columns.size();j++){
                  tr->GetField(columns[j])->print();
                  cout<<"  ";
                }
                cout<<endl;
              }
              return DB_SUCCESS;
            }
          }
        }
      }
    }
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }
    auto ptr_rows  = rec_sel(cond, *&origin_rows,tableinfo,current_db->catalog_mgr_);

    for(auto it=ptr_rows.begin();it!=ptr_rows.end();it++){
      for(uint32_t j=0;j<columns.size();j++){
        (*it)->GetField(columns[j])->print();
        cout<<"  ";
      }
      cout<<endl;
    }
    cout<<"Select Success, Affects "<<ptr_rows.size()<<" Record!"<<endl;
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif

  string table_name=ast->child_->val_;

  TableInfo *tableinfo = nullptr;
  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
    return DB_FAILED;
  }
  vector<Field> fields;
  pSyntaxNode column_pointer= ast->child_->next_->child_;//the head of inset values
  int cnt = tableinfo->GetSchema()->GetColumnCount();// the number of columns
  // cout<<"cnt:"<<cnt<<endl;
  for ( int i = 0 ; i < cnt ; i ++ ){
    TypeId now_type_id = tableinfo->GetSchema()->GetColumn(i)->GetType();
    if (column_pointer==nullptr){//tht end of all insert values
      for ( int j = i ; j < cnt ; j ++ ){
        //cout<<"has null!"<<endl;
        Field new_field(tableinfo->GetSchema()->GetColumn(j)->GetType());
        fields.push_back(new_field);
      }
      break;
    }
    if(column_pointer->val_==nullptr ){
      //cout<<"has null!"<<endl;
      Field new_field(now_type_id);
      fields.push_back(new_field);
    }
    else{
      //cout<<"a number"<<endl;
      if (now_type_id==kTypeInt){//����
        int x = atoi(column_pointer->val_);
        Field new_field (now_type_id,x);
        fields.push_back(new_field);
      }
      else if(now_type_id==kTypeFloat){//������
        float f = atof(column_pointer->val_);
        Field new_field (now_type_id,f);
        fields.push_back(new_field);
      }
      else {//�ַ���
        string s = column_pointer->val_;
        uint32_t len=tableinfo->GetSchema()->GetColumn(i)->GetLength();
        // cout<<"insert char length "<<len<<endl;
        char *c = new char[len];
        strcpy(c,s.c_str());
        Field new_field = Field(TypeId::kTypeChar, const_cast<char *>(c), s.size(), true);
        // Field new_field (now_type_id,c,len,true);
        fields.push_back(new_field);
      }
    }
    column_pointer = column_pointer->next_;
  }
  if (column_pointer!=nullptr){
    cout<<"Column Count doesn't match!"<<endl;
    return DB_FAILED;
  }
  Row row(fields);//构健一个row对象
  ASSERT(tableinfo!=nullptr,"TableInfo is Null!");
  TableHeap* tableheap=tableinfo->GetTableHeap();//得到堆表管理权
  bool Is_Insert=tableheap->InsertTuple(row,nullptr);
  if(Is_Insert==false){
    cout<<"Insert Failed, Affects 0 Record!"<<endl;
    return DB_FAILED;
  }else{
    vector <IndexInfo*> indexes;//得到所有索引的TableInfo
    current_db->catalog_mgr_->GetTableIndexes(table_name,indexes);

    for(auto p=indexes.begin();p<indexes.end();p++){
      //遍历所有的index
      IndexSchema* index_schema = (*p)->GetIndexKeySchema();
      vector<Field> index_fields;
      for(auto it:index_schema->GetColumns()){
        index_id_t tmp;
        if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS){
          index_fields.push_back(fields[tmp]);
        }
      }
      Row index_row(index_fields);
      dberr_t IsInsert=(*p)->GetIndex()->InsertEntry(index_row,row.GetRowId(),nullptr);
      //cout<<"RowID: "<<row.GetRowId().Get()<<endl;
      if(IsInsert==DB_FAILED){
        //插入失败
        cout<<"Insert Failed, Affects 0 Record!"<<endl;
        //把前面插入过的全都撤销掉
        for(auto q=indexes.begin();q!=p;q++){
          IndexSchema* index_schema_already = (*q)->GetIndexKeySchema();
          vector<Field> index_fields_already;
          for(auto it:index_schema_already->GetColumns()){
            index_id_t tmp_already;
            if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp_already)==DB_SUCCESS){
              index_fields_already.push_back(fields[tmp_already]);
            }
          }
          Row index_row_already(index_fields_already);
          (*q)->GetIndex()->RemoveEntry(index_row_already,row.GetRowId(),nullptr);
        }
        tableheap->MarkDelete(row.GetRowId(),nullptr);
        return IsInsert;
      }else{
        //cout<<"Insert Into Index Sccess"<<endl;
      }
    }
    //ȫ�����ܲ��ȥ���ܳɹ�����???
    cout<<"Insert Success, Affects 1 Record!"<<endl;
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  string table_name=ast->child_->val_;
  TableInfo *tableinfo = nullptr;
  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
    return DB_FAILED;
  }
  TableHeap* tableheap=tableinfo->GetTableHeap();//�õ�����Ӧ���ļ���
  auto del = ast->child_;
  vector<Row*> tar;

  if(del->next_==nullptr){//��ȡ����ѡ��������row������vector<Row*> tar��
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      tar.push_back(tp);
    }
  }
  else{
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }
    tar  = rec_sel(del->next_->child_, *&origin_rows,tableinfo,current_db->catalog_mgr_);
  }
  for(auto it:tar){
    tableheap->ApplyDelete(it->GetRowId(),nullptr);
  }
  cout<<"Delete Success, Affects "<<tar.size()<<" Record!"<<endl;
  vector <IndexInfo*> indexes;//���������������indexinfo
  current_db->catalog_mgr_->GetTableIndexes(table_name,indexes);
  // int cnt=0;
  for(auto p=indexes.begin();p!=indexes.end();p++){
    // cout<<"++++ "<<cnt++<<" ++++"<<endl;
    // int cnt1=0;
    for(auto j:tar){
      vector<Field> index_fields;

      for(auto it:(*p)->GetIndexKeySchema()->GetColumns()){
        index_id_t tmp;
        if(tableinfo->GetSchema()->GetColumnIndex(it->GetName(),tmp)==DB_SUCCESS){
          // cout<<cnt1++<<endl;
          index_fields.push_back(*j->GetField(tmp));
        }
      }
      Row index_row(index_fields);
      (*p)->GetIndex()->RemoveEntry(index_row,j->GetRowId(),nullptr);
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  string table_name=ast->child_->val_;
  TableInfo *tableinfo = nullptr;
  DBStorageEngine* current_db=dbs_.find(current_db_)->second;
  dberr_t GetRet = current_db->catalog_mgr_->GetTable(table_name, tableinfo);
  if (GetRet==DB_TABLE_NOT_EXIST){
    cout<<"Table Not Exist!"<<endl;
    return DB_FAILED;
  }
  TableHeap* tableheap=tableinfo->GetTableHeap();
  auto updates = ast->child_->next_;
  vector<Row*> tar;

  if(updates->next_==nullptr){
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      tar.push_back(tp);
    }

  }
  else{
    vector<Row*> origin_rows;
    for(auto it=tableinfo->GetTableHeap()->Begin(nullptr);it!=tableinfo->GetTableHeap()->End();it++){
      Row* tp = new Row(*it);
      origin_rows.push_back(tp);
    }
    tar  = rec_sel(updates->next_->child_, *&origin_rows,tableinfo,current_db->catalog_mgr_);

  }
  updates = updates->child_;
  SyntaxNode* tmp_up = updates;
  int flag=1;
  vector <IndexInfo*> indexes;
  current_db->catalog_mgr_->GetTableIndexes(tableinfo->GetTableName(),indexes);
  while(tmp_up && tmp_up->type_ == kNodeUpdateValue){//ֱcheck if any index is being undated
    string col = tmp_up->child_->val_;
    for(auto p=indexes.begin();p<indexes.end();p++){
      if((*p)->GetIndexKeySchema()->GetColumnCount()==1){
        if((*p)->GetIndexKeySchema()->GetColumns()[0]->GetName()==col){
          flag=0;
          break;
        }
      }
    }
    tmp_up = tmp_up->next_;
  }
  if(flag==0){
    cout<<"index cannot be updated!!"<<endl;
    return DB_SUCCESS;
  }

  while(updates && updates->type_ == kNodeUpdateValue){//ֱ���ս��???
    string col = updates->child_->val_;
    string upval = updates->child_->next_->val_;
    uint32_t index;//�ҵ�col��Ӧ��index
    tableinfo->GetSchema()->GetColumnIndex(col,index);
    TypeId tid = tableinfo->GetSchema()->GetColumn(index)->GetType();
    if(tid == kTypeInt){
      Field* newval = new Field(kTypeInt,stoi(upval));
      for(auto it:tar){
        it->GetFields()[index] = newval;
      }
    }
    else if(tid == kTypeFloat){
      Field* newval = new Field(kTypeFloat,stof(upval));
      for(auto it:tar){
        it->GetFields()[index] = newval;
      }
    }
    else if(tid == kTypeChar){
      uint32_t len = tableinfo->GetSchema()->GetColumn(index)->GetLength();
      char* tc = new char[len];
      strcpy(tc,upval.c_str());
      Field* newval = new Field(kTypeChar,tc,len,true);
      for(auto it:tar){
        it->GetFields()[index] = newval;
      }
    }
    updates = updates->next_;
  }
  for(auto it:tar){
    tableheap->UpdateTuple(*it,it->GetRowId(),nullptr);
  }
  cout<<"Update Success, Affects "<<tar.size()<<" Record!"<<endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}
#include<fstream>
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string name = ast->child_->val_;
  string file_name =name;
  //cout<<file_name;
  ifstream infile;
  infile.open(file_name.data());//Connect a file stream object to a file
  if (infile.is_open()){ //if open fails,return false
    string s;
    while(getline(infile,s)){//read line by line
      YY_BUFFER_STATE bp = yy_scan_string(s.c_str());
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }
      yy_switch_to_buffer(bp);

      MinisqlParserInit();

      yyparse();
      //ExecuteContext context;
      Execute(MinisqlGetParserRootNode(), context);
    }
    return DB_SUCCESS;
  }
  else{
    cout<<"Failed In Opening File!"<<endl;
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
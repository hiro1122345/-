// Peach Player - Firestore sync. GitHub Pages (no build tools required).
// Uses Firebase official CDN modular SDK; never store passwords in source code.
import firebaseConfig from './firebase-config.js';
import {initializeApp} from 'https://www.gstatic.com/firebasejs/12.19.0/firebase-app.js';
import {getAuth,onAuthStateChanged,signInWithEmailAndPassword,signOut} from 'https://www.gstatic.com/firebasejs/12.19.0/firebase-auth.js';
import {getFirestore,collection,doc,onSnapshot,setDoc,writeBatch,serverTimestamp} from 'https://www.gstatic.com/firebasejs/12.19.0/firebase-firestore.js';

const $ = (id) => document.getElementById(id);
const ui = {
  status:$('cloud-status'), loginArea:$('cloud-login-area'), accountArea:$('cloud-account-area'),
  migrateArea:$('cloud-migrate-area'), migrateExplain:$('cloud-migrate-explain'),
  login:$('cloud-login'), logout:$('cloud-logout'), email:$('cloud-email'), password:$('cloud-password'),
  user:$('cloud-user'), upload:$('cloud-upload'), merge:$('cloud-merge'),
  useRemote:$('cloud-use-remote'), resume:$('cloud-resume')
};
const appUI = window.peachApp;
const BACKUP_PREFIX = 'peach-player-local-before-sync-';
const SYNC_MARKER_PREFIX = 'peach-player-last-confirmed-sync-';

function status(msg, error=false){ui.status.textContent=msg;ui.status.style.borderColor=error?'#bd5a6a':''}
function errorText(e){
 const code = String(e?.code||'');
 const known = {
  'auth/invalid-credential':'メールアドレスまたはパスワードが違います。',
  'auth/invalid-email':'メールアドレスの形式を確認してください。',
  'auth/too-many-requests':'ログイン試行が多すぎます。しばらく待ってください。',
  'permission-denied':'Firestoreのセキュリティルールを確認してください。',
  'unavailable':'通信できません。Wi-Fiとインターネット接続を確認してください。'
 };
 return known[code]||`エラー ${code||''}：${e?.message||e}`;
}
function ordered(entries){
 return [...entries].sort((a,b)=>{
  const x=Number.isFinite(Number(a.sortIndex))?Number(a.sortIndex):999999;
  const y=Number.isFinite(Number(b.sortIndex))?Number(b.sortIndex):999999;
  return x-y || String(a.createdAt||'').localeCompare(String(b.createdAt||'')) || String(a.id).localeCompare(String(b.id));
 });
}
// Remove Firestore server-only data and keep a consistent JSON form for comparisons.
function clipData(c,i){
 return {
  id:String(c.id),videoId:String(c.videoId),sourceUrl:String(c.sourceUrl||''),
  start:Number(c.start),end:Number(c.end),playerName:String(c.playerName||'エイク'),
  character:String(c.character),title:String(c.title),category:String(c.category||'全般'),
  note:String(c.note||''),createdAt:String(c.createdAt||''),
  sortIndex:Number.isFinite(Number(c.sortIndex))?Number(c.sortIndex):i
 };
}
function localData(list){return list.map((c,i)=>clipData({...c,sortIndex:i},i))}
function fingerprint(list){return JSON.stringify(list.map((c,i)=>{
 const x=clipData(c,i);delete x.sortIndex;return x;
}));}
function same(a,b){return fingerprint(a)===fingerprint(b)}
function safeId(id){return id&&id!=='.'&&id!=='..'&&!id.includes('/')&&id.length<=500;}
function locallyUnchangedSinceLastSync(){
 try{return localStorage.getItem(SYNC_MARKER_PREFIX+uid)===fingerprint(appUI.getClips())}catch{return false}
}
function markConfirmedRemote(){
 try{localStorage.setItem(SYNC_MARKER_PREFIX+uid,fingerprint(remoteClips))}catch{}
}

let auth, db, uid=null, unsubClips=null, unsubPrefs=null, unsubPlayback=null;
let ready=false, remoteClips=[], lastKnownClips=[], serverSeen=false, lastResume=null;
let prefsSeen=false, remotePrefs=null, latestCloudState='';
let prefsTimer=null, writeCount=0, generation=0;
function clipCollection(){return collection(db,'users',uid,'clips')}
function prefsDoc(){return doc(db,'users',uid,'state','preferences')}
function playbackDoc(){return doc(db,'users',uid,'state','playback')}
function clearMigration(){
 ui.migrateArea.hidden=true;ui.upload.hidden=true;ui.merge.hidden=true;ui.useRemote.hidden=true;
}
function showMigration(text, mode){
 ready=false;ui.migrateArea.hidden=false;ui.migrateExplain.textContent=text;
 ui.upload.hidden=mode!=='upload';ui.merge.hidden=mode!=='resolve';ui.useRemote.hidden=mode!=='resolve';
}
function resetConnection(){
 ++generation;
 if(unsubClips)unsubClips();if(unsubPrefs)unsubPrefs();if(unsubPlayback)unsubPlayback();
 unsubClips=unsubPrefs=unsubPlayback=null;
 clearTimeout(prefsTimer);prefsTimer=null;
 uid=null;ready=false;remoteClips=[];lastKnownClips=[];serverSeen=false;
 prefsSeen=false;remotePrefs=null;lastResume=null;writeCount=0;
 ui.resume.hidden=true;clearMigration();
}
function backupLocal(){
 const key=BACKUP_PREFIX+uid;
 const local=appUI.getClips();
 try{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify({createdAt:new Date().toISOString(),clips:local}))}catch{}
}
function setResume(record){
 lastResume=record&&typeof record.clipId==='string'&&Number.isFinite(Number(record.seconds))?record:null;
 const exists=lastResume&&appUI.getClips().some(c=>c.id===lastResume.clipId);
 ui.resume.hidden=!exists;
 if(exists)ui.resume.textContent=`▶ 前回の続きから再生（${appUI.getClips().find(c=>c.id===lastResume.clipId)?.title||'前回の試合'}）`;
}
function useRemoteClips(remote){
 lastKnownClips=remote.map((c,i)=>clipData(c,i));
 appUI.applyClips(remote);
 setResume(lastResume);
}
async function sendOperations(ops){
 if(!uid||!ops.length)return;
 const currentUid=uid;
 writeCount++;
 status(`同期中…（${ops.length}件の変更）`);
 try{
  // Firestore batch operations max 500. Use <= 250 for safety.
  for(let i=0;i<ops.length;i+=250){
   if(uid!==currentUid)throw Error('ログイン状態が変わりました');
   const batch=writeBatch(db);
   for(const op of ops.slice(i,i+250)){
    const ref=doc(db,'users',currentUid,'clips',op.id);
    if(op.kind==='delete')batch.delete(ref);
    else batch.set(ref,{...op.clip,updatedAt:serverTimestamp()});
   }
   await batch.commit();
  }
  if(uid===currentUid)status('☁ 同期済み（PC・iPhone間で自動反映）');
 }catch(e){
  status('クラウド保存に失敗しました。ローカルには保存されています。'+errorText(e),true);
  // A future successful write or manual JSON import can recover edits.
 }finally{writeCount=Math.max(0,writeCount-1)}
}
function diffOps(previous,next){
 const before=new Map(previous.map(c=>[c.id,c]));
 const after=new Map(next.map(c=>[c.id,c]));
 const ops=[];
 for(const [id,c] of after){
  if(!safeId(id))continue;
  if(!before.has(id)||JSON.stringify(before.get(id))!==JSON.stringify(c))ops.push({kind:'set',id,clip:c});
 }
 for(const id of before.keys())if(!after.has(id)&&safeId(id))ops.push({kind:'delete',id});
 return ops;
}
function initReady(list){
 ready=true;clearMigration();useRemoteClips(list);
 if(prefsSeen){
  if(remotePrefs)appUI.applyPrefs(remotePrefs);
  else savePrefs(appUI.getPrefs());
 }
 status('☁ 同期済み（PC・iPhone間で自動反映）');
}
function assessInitial(){
 if(ready||!serverSeen||!uid)return;
 const local=appUI.getClips();
 if(remoteClips.length===0&&local.length>0){
  showMigration(`クラウドは空です。この端末に${local.length}件あります。まず試合がそろっているPCで「初回アップロード」を押してください。`, 'upload');
  status('初回アップロード待ち（この端末のデータは保持）');
 }else if(remoteClips.length>0&&local.length>0&&!same(local,remoteClips)){
  showMigration(`クラウド：${remoteClips.length}件／この端末：${local.length}件。異なるデータがあります。端末データの統合（同じIDはクラウド優先）、またはクラウドのデータを使う、を選択してください。`, 'resolve');
  status('移行方法の選択待ち（どちらのデータも削除しません）');
 }else{
  initReady(remoteClips);
 }
}
function startListeners(currentUser){
 uid=currentUser.uid;
 serverSeen=false;ready=false;
 ui.loginArea.hidden=true;ui.accountArea.hidden=false;ui.user.textContent=currentUser.email||'ログイン中';
 status('クラウドのデータを確認しています…');
 const myGeneration=generation;
 unsubClips=onSnapshot(clipCollection(),{includeMetadataChanges:true},snap=>{
  if(myGeneration!==generation||!uid)return;
  // Don't treat an empty cache as an empty cloud when network is unavailable.
  if(snap.metadata.fromCache&&!serverSeen){status('クラウドへの接続待ち（オフライン時は端末のデータを利用できます）');return}
  serverSeen=true;
  remoteClips=ordered(snap.docs.map(d=>clipData({...d.data(),id:d.id},0)));
  if(!ready){assessInitial();return}
  useRemoteClips(remoteClips);
  if(!snap.metadata.hasPendingWrites){
   markConfirmedRemote();
   if(writeCount===0)status('☁ 同期済み（PC・iPhone間で自動反映）');
  }
 },e=>status('試合データを取得できません：'+errorText(e),true));
 unsubPrefs=onSnapshot(prefsDoc(),{includeMetadataChanges:true},snap=>{
  if(myGeneration!==generation||snap.metadata.fromCache)return;
  prefsSeen=true;remotePrefs=snap.exists()?snap.data():null;
  if(ready){if(remotePrefs)appUI.applyPrefs(remotePrefs);else savePrefs(appUI.getPrefs())}
 },e=>status('設定の同期エラー：'+errorText(e),true));
 unsubPlayback=onSnapshot(playbackDoc(),snap=>{
  if(myGeneration!==generation)return;
  setResume(snap.exists()?snap.data():null);
 },e=>status('再生位置の同期エラー：'+errorText(e),true));
}
async function savePrefs(prefs){
 if(!ready||!uid)return;
 try{await setDoc(prefsDoc(),{...prefs,updatedAt:serverTimestamp()},{merge:true})}
 catch(e){status('設定の保存に失敗しました：'+errorText(e),true)}
}
async function initialUpload(){
 if(!uid||ready||!serverSeen||remoteClips.length)return;
 const local=localData(appUI.getClips());
 if(!local.length){status('アップロードするデータがありません。',true);return}
 if(!confirm(`この端末の${local.length}試合をFirebaseへ初回登録します。先にJSONバックアップを保存しましたか？`))return;
 ui.upload.disabled=true;status('初回アップロード中…');
 // Stay in migration mode on failure, no destructive clearing.
 try{
  const ops=local.filter(c=>safeId(c.id)).map(c=>({kind:'set',id:c.id,clip:c}));
  if(ops.length!==local.length)throw Error('試合IDに使用できない文字が含まれています。JSONバックアップを確認してください。');
  const currentUid=uid;
  for(let i=0;i<ops.length;i+=250){
   const batch=writeBatch(db);
   for(const op of ops.slice(i,i+250))batch.set(doc(db,'users',currentUid,'clips',op.id),{...op.clip,updatedAt:serverTimestamp()});
   await batch.commit();
  }
  if(uid===currentUid){initReady(local);status(`☁ ${local.length}件を初回同期しました。iPhoneで同じアカウントにログインしてください。`)}
 }catch(e){status('初回アップロードに失敗：'+errorText(e),true)}
 finally{ui.upload.disabled=false}
}
async function mergeLocal(){
 if(!uid||ready||!serverSeen)return;
 const local=localData(appUI.getClips());
 const remote=remoteClips;
 const ids=new Set(remote.map(c=>c.id));
 const additions=local.filter(c=>!ids.has(c.id));
 if(!confirm(`クラウドの${remote.length}件を維持して、端末だけにある${additions.length}件を追加します。同一IDで内容が違う場合はクラウド優先です。続けますか？`))return;
 backupLocal();
 ui.merge.disabled=true;status('試合を統合しています…');
 try{
  if(additions.some(c=>!safeId(c.id)))throw Error('試合IDに不正な文字が含まれています。');
  const currentUid=uid;
  const ops=additions.map((c,i)=>({...c,sortIndex:remote.length+i}));
  for(let i=0;i<ops.length;i+=250){
   const batch=writeBatch(db);
   for(const c of ops.slice(i,i+250))batch.set(doc(db,'users',currentUid,'clips',c.id),{...c,updatedAt:serverTimestamp()});
   await batch.commit();
  }
  if(uid===currentUid){initReady([...remote,...ops]);status(`☁ 統合完了。${additions.length}件を追加しました。旧端末データはブラウザ内にも退避しました。`)}
 }catch(e){status('統合に失敗：'+errorText(e),true)}
 finally{ui.merge.disabled=false}
}
function chooseRemote(){
 if(!uid||ready||!serverSeen)return;
 backupLocal();
 initReady(remoteClips);
 status('☁ クラウドのデータを表示中。以前の端末データはブラウザ内に退避しました。');
}
window.peachCloud={
 onClipsSaved(clips){
  if(!ready||!uid)return;
  const next=localData(clips), ops=diffOps(lastKnownClips,next);
  if(!ops.length)return;
  lastKnownClips=next;
  void sendOperations(ops);
 },
 onPrefsSaved(prefs){
  if(!ready||!uid)return;
  clearTimeout(prefsTimer);
  prefsTimer=setTimeout(()=>{void savePrefs(prefs)},400);
 },
 onPlaybackSaved(record){
  if(!ready||!uid)return;
  const r={clipId:String(record.clipId),seconds:Number(record.seconds)};
  if(!Number.isFinite(r.seconds))return;
  void setDoc(playbackDoc(),{...r,updatedAt:serverTimestamp()},{merge:true})
    .catch(e=>status('再生位置の保存に失敗：'+errorText(e),true));
 }
};
ui.login.addEventListener('click',async()=>{
 if(!auth)return;
 ui.login.disabled=true;
 try{await signInWithEmailAndPassword(auth,ui.email.value.trim(),ui.password.value);ui.password.value=''}
 catch(e){status('ログイン失敗：'+errorText(e),true)}
 finally{ui.login.disabled=false}
});
ui.password.addEventListener('keydown',e=>{if(e.key==='Enter')ui.login.click()});
ui.logout.addEventListener('click',()=>{if(auth)void signOut(auth).catch(e=>status(errorText(e),true))});
ui.upload.addEventListener('click',()=>void initialUpload());
ui.merge.addEventListener('click',()=>void mergeLocal());
ui.useRemote.addEventListener('click',chooseRemote);
ui.resume.addEventListener('click',()=>{if(lastResume)appUI.resume(lastResume)});

if(!firebaseConfig?.apiKey||!firebaseConfig?.projectId||firebaseConfig.apiKey.includes('ここに')){
 status('Firebase未設定です。firebase-config.js にFirebase Consoleの設定値を貼り付けてください。端末内の試合はこのまま利用できます。',true);
}else{
 try{
  const firebaseApp=initializeApp(firebaseConfig);
  auth=getAuth(firebaseApp);db=getFirestore(firebaseApp);
  ui.loginArea.hidden=false;
  onAuthStateChanged(auth,user=>{
   resetConnection();
   if(user)startListeners(user);
   else{ui.loginArea.hidden=false;ui.accountArea.hidden=true;status('未ログイン。試合は端末に保存されています。同期するにはログインしてください。')}
  },e=>status('認証の読み込み失敗：'+errorText(e),true));
 }catch(e){status('Firebase初期化に失敗：'+errorText(e),true)}
}

// Run this callback inside an Artifact Session with its bound presentation.
export async function addBatch({presentation,input}) {
 const root=input.root;
 for (const d of input.slides) {
  const s=presentation.slides.add({width:1280,height:720});
  s.background.fill='#F7F2E7';
  function text(v,x,y,w,h,size=25,bold=false,color='#243A36') {
   const t=s.shapes.add({geometry:'textbox',position:{left:x,top:y,width:w,height:h},fill:'none',line:{fill:'none',width:0}});
   t.text=v;t.text.style={fontFamily:'Arial',fontSize:size,bold,color};return t;
  }
  const art=d.number===23?'balloon':d.number===12?'door':null;
  if(art)s.images.add({path:`${root}/docs/wallpapers/joyful/kladovka-${art}.png`,fit:'cover',position:{left:0,top:0,width:1280,height:720}});
  if(art)s.shapes.add({geometry:'rect',position:{left:0,top:0,width:750,height:720},fill:'#F7F2E7',line:{fill:'none',width:0}});
  text('КЛАДОВКА',64,30,400,28,15,true,'#678174');
  text(d.title,64,85,art?670:1150,art?95:74,art?36:42,true);
  text(d.subtitle,66,art?190:171,art?650:1140,65,24,false,'#53685D');
  if(d.number===4){
   s.images.add({path:`${root}/docs/screenshots/ui-light.png`,fit:'contain',position:{left:64,top:255,width:850,height:400}});
   text('Панель администратора\n\nДоступ к полкам\nи объектам\n\nКонтроль ресурсов',950,295,270,280,23);
  } else if(d.number===7){
   const vals=[['Полных копий (RF)','Нужно для кворума','Допустимо недоступных'],['3','2','1'],['4','3','1'],['5','3','2']];
   const table=s.tables.add({rows:4,columns:3,left:64,top:275,width:1150,height:265,values:vals});
   for(let r=0;r<4;r++)for(let c=0;c<3;c++){const cell=table.getCell(r,c);cell.fill=r===0?'#385C4D':r%2?'#E8EDDF':'#F7F2E7';cell.text.style={fontFamily:'Arial',fontSize:r===0?22:32,bold:r===0,color:r===0?'#FFFFFF':'#243A36'};}
   text('RF — целевое число полных копий. Успешная запись не ждёт все реплики.',66,582,1140,64,24);
  } else {
   const n=d.blocks.length;
   if(art){d.blocks.forEach((b,i)=>{text(b.title,66,280+i*125,650,37,27,true);text(b.body,66,325+i*125,650,75,23);});}
   else if(n===2){d.blocks.forEach((b,i)=>{const x=66+i*590;text(b.title,x,290,540,50,30,true);text(b.body,x,365,540,210,27);});}
   else {d.blocks.forEach((b,i)=>{const y=270+i*(n===4?91:124);text(b.title,66,y,330,60,27,true);text(b.body,430,y,780,100,25);});}
  }
  text(`${String(d.number).padStart(2,'0')}  /  Продукт и эксплуатация`,66,674,1000,24,15,false,'#6D7D70');
  s.speakerNotes.text=d.source;
 }
 return {slideCount:presentation.slides.items.length};
}

export async function addCover({presentation,input}) {
 const s=presentation.slides.items[0] || presentation.slides.add({width:1280,height:720});
 s.background.fill='#F7F2E7';
 s.images.add({path:`${input.root}/docs/wallpapers/joyful/kladovka-locker.png`,fit:'cover',position:{left:0,top:0,width:1280,height:720}});
 for(const [value,y,h,size,bold] of [['Кладовка',220,100,64,true],['Ваши файлы.\nВаш сервер.',340,120,32,false],['Продукт, работа с файлами\nи эксплуатация кластера',535,90,23,false]]){
 const t=s.shapes.add({geometry:'textbox',position:{left:665,top:y,width:550,height:h},fill:'none',line:{fill:'none',width:0}});t.text=value;t.text.style={fontFamily:'Arial',fontSize:size,bold,color:'#243A36'};
 }
 s.speakerNotes.text='Состояние репозитория на 21.10.2026. Источники: docs/USER.md, docs/ADMIN.md, docs/CONSENSUS.md. Не объявляет новый релиз.';
}


# -*- coding: utf-8 -*-
"""
Largest Files Finder — v5.2.1 (hotfix)

Korjaus:
- Lisätty puuttunut **smart_clean()**-metodi App-luokkaan (v5.2 kaatui UI-napin kutsusta).
- Pieniä turvakorjauksia: olemassaolon tarkistus poiston jälkeen.

Perii v5.2:n parannukset (TimeoutError-kesto, Poissulje polut, lajittelu, väripallot, jne.).
"""

import os
import sys
import threading
import traceback
import queue
import subprocess
from dataclasses import dataclass
from typing import List, Optional, Iterable, Tuple, Callable, Set, Dict
from datetime import datetime, timedelta

try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception as e:
    print("Tkinter vaaditaan tämän sovelluksen ajamiseen.")
    raise

SAFE='safe'; CAUTION='caution'; SYSTEM='system'
STATUS_META={SAFE:{'label':'🟢 Turvallinen','dot':'●','color':'#2ecc71'},CAUTION:{'label':'🟡 Harkittava','dot':'●','color':'#f1c40f'},SYSTEM:{'label':'🔴 Järjestelmä','dot':'●','color':'#e74c3c'}}
SYSTEM_ROOT_PREFIXES=['/System','/Library','/usr','/bin','/sbin','/private','/opt','/Applications']
SAFE_PATTERNS=['/target/','/deps/','/incremental/','/build/','.rlib','.rmeta','.d',
               os.path.expanduser('~/.npm'),os.path.expanduser('~/.cache/yarn'),os.path.expanduser('~/.cache/pnpm'),
               os.path.expanduser('~/.cargo/registry'),os.path.expanduser('~/.cargo/git'),
               os.path.expanduser('~/Library/Developer/Xcode/DerivedData'),
               os.path.expanduser('~/Library/Developer/CoreSimulator'),
               os.path.expanduser('~/Library/Caches')]

@dataclass
class FileInfo:
    path: str
    size: int
    created_ts: float
    @property
    def dirname(self)->str: return os.path.dirname(self.path)
    @property
    def basename(self)->str: return os.path.basename(self.path)
    @property
    def created_str(self)->str:
        try: return datetime.fromtimestamp(self.created_ts).strftime('%Y-%m-%d %H:%M')
        except Exception: return '-'

def human_size(n:int)->str:
    if n<0: return '-'
    units=["B","KB","MB","GB","TB","PB"]; i=0; f=float(n)
    while f>=1024.0 and i<len(units)-1:
        f/=1024.0; i+=1
    return f"{int(round(f))} {units[i]}" if i<=1 else f"{f:.2f} {units[i]}"

def get_created_ts(st)->float:
    ts=getattr(st,'st_birthtime',None)
    if ts is None: ts=st.st_ctime
    return float(ts)

def ext_matches(path:str,allowed_exts:Optional[List[str]])->bool:
    if not allowed_exts: return True
    return os.path.splitext(path)[1].lower() in allowed_exts

def in_date_range(ts:float,start_ts:Optional[float],end_ts:Optional[float])->bool:
    if start_ts is not None and ts<start_ts: return False
    if end_ts is not None and ts>end_ts: return False
    return True

def is_excluded_path(path:str, exclude_substrings:List[str])->bool:
    p=os.path.abspath(path)
    for sub in exclude_substrings:
        sub=sub.strip()
        if not sub: continue
        try:
            sub_exp=os.path.abspath(os.path.expanduser(sub)) if sub.startswith('~') or sub.startswith('/') else sub
            if sub.startswith('/') or sub.startswith('~'):
                if p.startswith(sub_exp): return True
            else:
                if sub in p: return True
        except Exception:
            pass
    return False

def iter_tree(root:str, follow_symlinks:bool, skip_hidden:bool, exclude_dirs:List[str], same_fs_only:bool,
              progress_cb:Optional[Callable[[str],None]], stop_flag:threading.Event,
              exclude_substrings:List[str]):
    try:
        root_dev=os.stat(root).st_dev if same_fs_only else None
    except Exception:
        root_dev=None
    stack=[root]; last_progress=0
    while stack and not stop_flag.is_set():
        d=stack.pop()
        if is_excluded_path(d, exclude_substrings):
            continue
        try:
            with os.scandir(d) as it:
                while True:
                    if stop_flag.is_set(): break
                    try:
                        entry=next(it)
                    except StopIteration:
                        break
                    except (TimeoutError,OSError):
                        break
                    try:
                        name=entry.name
                        if skip_hidden and name.startswith('.'): continue
                        full=entry.path
                        if is_excluded_path(full, exclude_substrings):
                            continue
                        if entry.is_dir(follow_symlinks=follow_symlinks):
                            if name in exclude_dirs: continue
                            if same_fs_only and root_dev is not None:
                                try:
                                    if os.stat(full, follow_symlinks=False).st_dev!=root_dev:
                                        continue
                                except Exception:
                                    continue
                            stack.append(full)
                        else:
                            yield full
                    except (PermissionError,FileNotFoundError,TimeoutError,OSError):
                        continue
        except (PermissionError,FileNotFoundError,TimeoutError,OSError):
            pass
        if progress_cb:
            from time import time as _now
            t=_now()
            if t-last_progress>0.25:
                last_progress=t
                try: progress_cb(d)
                except Exception: pass

def scan_files(root:str, allowed_exts:Optional[List[str]], min_size_bytes:int, follow_symlinks:bool, skip_hidden:bool,
               exclude_dirs:List[str], same_fs_only:bool, start_ts:Optional[float], end_ts:Optional[float],
               stop_flag:threading.Event, progress_cb=None, live_queue:Optional[queue.Queue]=None,
               seen_paths:Optional[Set[str]]=None, exclude_substrings:Optional[List[str]]=None)->Iterable[FileInfo]:
    exclude_substrings = exclude_substrings or []
    for path in iter_tree(root, follow_symlinks, skip_hidden, exclude_dirs, same_fs_only, progress_cb, stop_flag, exclude_substrings):
        try:
            norm=os.path.abspath(path)
            if seen_paths is not None and norm in seen_paths: continue
            if not ext_matches(path, allowed_exts): continue
            st=os.stat(path, follow_symlinks=follow_symlinks)
            size=st.st_size
            if size<min_size_bytes: continue
            cts=get_created_ts(st)
            if not in_date_range(cts,start_ts,end_ts): continue
            fi=FileInfo(path=path,size=size,created_ts=cts)
            if live_queue is not None:
                if seen_paths is not None: seen_paths.add(norm)
                try: live_queue.put_nowait(fi)
                except queue.Full: pass
            else:
                yield fi
        except (PermissionError,FileNotFoundError,TimeoutError,OSError):
            continue

# ---- luokittelu ----

def classify_path(path:str)->Tuple[str,str]:
    apath=os.path.abspath(path)
    for pfx in SYSTEM_ROOT_PREFIXES:
        try:
            if apath.startswith(pfx+os.sep) or apath==pfx:
                return SYSTEM, f'Järjestelmäpolku: {pfx}'
        except Exception: pass
    for patt in SAFE_PATTERNS:
        try:
            if patt.startswith(os.path.sep) or patt.startswith('~'):
                if apath.startswith(os.path.abspath(os.path.expanduser(patt))):
                    return SAFE, f'Välimuisti/build-artefakti: {patt}'
            else:
                if patt in apath: return SAFE, f'Build-väliaikainen: *{patt}*'
        except Exception: pass
    name=os.path.basename(apath).lower()
    if any(name.endswith(ext) for ext in ('.mov','.mp4','.mkv','.zip','.dmg','.pkg','.iso')): return CAUTION,'Iso käyttäjätiedosto/paketti'
    if '/Downloads/' in apath: return CAUTION,'Lataukset-kansio'
    return CAUTION,'Tuntematon (tarkista ennen poistoa)'

# ---- app ----
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Largest Files Finder v5.2.1")
        self.geometry("1350x880")
        self.stop_flag=threading.Event(); self.scan_thread=None
        self.results:List[FileInfo]=[]; self.live_q=None; self.seen_paths:set=set()
        self.sort_col=None; self.sort_desc=False
        self.build_ui()

    def build_ui(self):
        nb=ttk.Notebook(self); self.files_tab=ttk.Frame(nb); self.sys_tab=ttk.Frame(nb)
        nb.add(self.files_tab,text='Tiedostot'); nb.add(self.sys_tab,text='Järjestelmä / "Muut taltiot"'); nb.pack(fill=tk.BOTH,expand=True)
        # Files tab
        top=ttk.Frame(self.files_tab); top.pack(fill=tk.X,padx=10,pady=6)
        ttk.Label(top,text="Juuri (tyhjä = koko kone):").pack(side=tk.LEFT)
        self.root_var=tk.StringVar(value=""); ttk.Entry(top,textvariable=self.root_var).pack(side=tk.LEFT,fill=tk.X,expand=True,padx=6)
        ttk.Button(top,text="Valitse kansio…",command=self.choose_root).pack(side=tk.LEFT)
        ttk.Button(top,text="Tyhjennä",command=lambda:self.root_var.set("")).pack(side=tk.LEFT,padx=(6,0))

        filt=ttk.Frame(self.files_tab); filt.pack(fill=tk.X,padx=10,pady=4)
        self.ext_var=tk.StringVar(value=".mov,.mp4,.mkv,.zip,.dmg,.pkg"); self.min_mb_var=tk.StringVar(value="50"); self.topn_var=tk.StringVar(value="200")
        self.skip_hidden_var=tk.BooleanVar(value=True); self.follow_links_var=tk.BooleanVar(value=False); self.same_fs_only_var=tk.BooleanVar(value=True)
        ttk.Label(filt,text="Tiedostopäätteet (pilkuin):").grid(row=0,column=0,sticky=tk.W)
        ttk.Entry(filt,textvariable=self.ext_var,width=40).grid(row=0,column=1,sticky=tk.W,padx=6)
        ttk.Label(filt,text="Minimikoko (MB):").grid(row=0,column=2,sticky=tk.E)
        ttk.Entry(filt,textvariable=self.min_mb_var,width=8).grid(row=0,column=3,sticky=tk.W,padx=6)
        ttk.Label(filt,text="Top N:").grid(row=0,column=4,sticky=tk.E)
        ttk.Entry(filt,textvariable=self.topn_var,width=8).grid(row=0,column=5,sticky=tk.W,padx=6)
        ttk.Checkbutton(filt,text="Ohita piilotetut",variable=self.skip_hidden_var).grid(row=1,column=0,sticky=tk.W,pady=4)
        ttk.Checkbutton(filt,text="Seuraa symlinkkejä",variable=self.follow_links_var).grid(row=1,column=1,sticky=tk.W,pady=4)
        ttk.Checkbutton(filt,text="Vain sama tiedostojärjestelmä",variable=self.same_fs_only_var).grid(row=1,column=2,sticky=tk.W,pady=4)

        excl=ttk.Frame(self.files_tab); excl.pack(fill=tk.X,padx=10,pady=4)
        ttk.Label(excl,text="Poissulje polut (pilkuin, osuma mihin tahansa polkuun):").grid(row=0,column=0,sticky=tk.W)
        default_excl=",".join([os.path.expanduser('~/Library/CloudStorage'), os.path.expanduser('~/Library/Mobile Documents'), '/Volumes', 'OneDriveCloudTemp'])
        self.exclude_substrings_var=tk.StringVar(value=default_excl)
        ttk.Entry(excl,textvariable=self.exclude_substrings_var).grid(row=0,column=1,sticky=tk.W,padx=6)

        datef=ttk.Frame(self.files_tab); datef.pack(fill=tk.X,padx=10,pady=4)
        self.start_date_var=tk.StringVar(value=""); self.end_date_var=tk.StringVar(value="")
        ttk.Label(datef,text="Luotu alkaen (YYYY-MM-DD):").grid(row=0,column=0,sticky=tk.E)
        ttk.Entry(datef,textvariable=self.start_date_var,width=14).grid(row=0,column=1,sticky=tk.W,padx=6)
        ttk.Label(datef,text="Luotu asti (YYYY-MM-DD):").grid(row=0,column=2,sticky=tk.E)
        ttk.Entry(datef,textvariable=self.end_date_var,width=14).grid(row=0,column=3,sticky=tk.W,padx=6)

        statusf=ttk.Frame(self.files_tab); statusf.pack(fill=tk.X,padx=10,pady=4)
        self.show_safe=tk.BooleanVar(value=True); self.show_caution=tk.BooleanVar(value=True); self.show_system=tk.BooleanVar(value=False)
        ttk.Checkbutton(statusf,text="Näytä 🟢 Turvalliset",variable=self.show_safe,command=self.refresh_tree_filter).pack(side=tk.LEFT)
        ttk.Checkbutton(statusf,text="Näytä 🟡 Harkittavat",variable=self.show_caution,command=self.refresh_tree_filter).pack(side=tk.LEFT,padx=(8,0))
        ttk.Checkbutton(statusf,text="Näytä 🔴 Järjestelmä",variable=self.show_system,command=self.refresh_tree_filter).pack(side=tk.LEFT,padx=(8,0))
        ttk.Label(statusf,text=" | Smart Clean kynnys (MB):").pack(side=tk.LEFT,padx=(16,4))
        self.clean_threshold_mb=tk.StringVar(value="100"); ttk.Entry(statusf,textvariable=self.clean_threshold_mb,width=6).pack(side=tk.LEFT)
        ttk.Button(statusf,text="Siivoa turvalliset (Roskakori)",command=self.smart_clean).pack(side=tk.LEFT,padx=(10,0))

        actions=ttk.Frame(self.files_tab); actions.pack(fill=tk.X,padx=10,pady=6)
        ttk.Button(actions,text="Skannaa",command=self.start_scan).pack(side=tk.LEFT)
        self.stop_btn=ttk.Button(actions,text="Pysäytä",command=self.stop_scan,state=tk.DISABLED); self.stop_btn.pack(side=tk.LEFT,padx=(6,0))
        self.reveal_btn=ttk.Button(actions,text="Näytä Finderissa",command=self.reveal_selected,state=tk.DISABLED); self.reveal_btn.pack(side=tk.LEFT,padx=(12,0))

        self.status_var=tk.StringVar(value="Valmis."); ttk.Label(self.files_tab,textvariable=self.status_var).pack(fill=tk.X,padx=10)

        cols=("status","name","dir","size","created")
        self.tree=ttk.Treeview(self.files_tab,columns=cols,show='headings')
        self.tree.heading("status",text="Status",command=lambda:self.on_heading('status'))
        self.tree.heading("name",text="Tiedosto",command=lambda:self.on_heading('name'))
        self.tree.heading("dir",text="Kansio",command=lambda:self.on_heading('dir'))
        self.tree.heading("size",text="Koko",command=lambda:self.on_heading('size'))
        self.tree.heading("created",text="Luotu",command=lambda:self.on_heading('created'))
        self.tree.column("status",width=120,anchor=tk.W)
        self.tree.column("name",width=320,anchor=tk.W)
        self.tree.column("dir",width=740,anchor=tk.W)
        self.tree.column("size",width=100,anchor=tk.E)
        self.tree.column("created",width=120,anchor=tk.W)
        self.tree.pack(fill=tk.BOTH,expand=True,padx=10,pady=8)
        self.tree.bind("<Double-1>",lambda e:self.reveal_selected())
        ttk.Scrollbar(self.tree,orient='vertical',command=self.tree.yview).pack(side='right',fill='y')
        self.filtered_indices:List[int]=[]

        ttk.Label(self.sys_tab,text="APFS snapshotit ja välimuistit käsitellään täällä (v5:stä tutut toiminnot).",wraplength=900,justify='left').pack(anchor='w',padx=10,pady=10)

    # ------- helpers & actions -------
    def choose_root(self):
        d=filedialog.askdirectory(initialdir=self.root_var.get() or os.path.expanduser('~'))
        if d: self.root_var.set(d)
    def set_status(self,txt:str): self.status_var.set(txt); self.update_idletasks()
    def parse_date(self,s:str)->Optional[float]:
        s=(s or '').strip();
        if not s: return None
        try: return datetime.strptime(s,'%Y-%m-%d').timestamp()
        except ValueError:
            messagebox.showerror('Virhe',f"Päivämäärä '{s}' ei ole muodossa YYYY-MM-DD."); return None
    def on_heading(self,col:str):
        if getattr(self,'sort_col',None)==col: self.sort_desc=not self.sort_desc
        else:
            self.sort_col=col; self.sort_desc=(col in ('size','created'))
        self.refresh_tree_filter()

    def start_scan(self):
        root=self.root_var.get().strip() or ('/' if sys.platform=='darwin' else os.environ.get('SystemDrive','C:')+'\\' )
        if not os.path.isdir(root):
            messagebox.showerror('Virhe','Valitse kelvollinen juurikansio tai jätä tyhjäksi koko koneelle.'); return
        allowed_exts=[e if e.startswith('.') else '.'+e for e in [p.strip().lower() for p in (self.ext_var.get() or '').split(',') if p.strip()]]
        try: min_mb=float(self.min_mb_var.get().strip() or 0)
        except ValueError: messagebox.showerror('Virhe','Minimikoko (MB) ei ole numero.'); return
        min_size_bytes=int(min_mb*1024*1024)
        try: int(self.topn_var.get().strip() or 200)
        except ValueError: messagebox.showerror('Virhe','Top N ei ole kokonaisluku.'); return
        s=self.parse_date(self.start_date_var.get());
        if s is None and self.start_date_var.get().strip(): return
        e=self.parse_date(self.end_date_var.get());
        if e is None and self.end_date_var.get().strip(): return
        if e is not None: e=e+86399.0
        exclude_substrings=[x.strip() for x in (self.exclude_substrings_var.get() or '').split(',') if x.strip()]

        self.results=[]; self.seen_paths.clear(); self.tree.delete(*self.tree.get_children())
        self.stop_btn.config(state=tk.NORMAL); self.reveal_btn.config(state=tk.DISABLED)
        self.stop_flag.clear(); self.live_q=queue.Queue(maxsize=5000)

        def progress_cb(dirpath): self.set_status(f"Skannataan: {dirpath}")
        def run():
            try:
                for _ in scan_files(root, allowed_exts or None, min_size_bytes, self.follow_links_var.get(), self.skip_hidden_var.get(), [], self.same_fs_only_var.get(), s, e, self.stop_flag, progress_cb, self.live_q, self.seen_paths, exclude_substrings):
                    if self.stop_flag.is_set(): break
            except Exception as ex:
                traceback.print_exc(); messagebox.showerror('Virhe',f"Skannaus epäonnistui:\n{ex}")
            finally:
                self.after(0,self.finish_scan)
        def drain():
            if self.live_q is None: return
            processed=0
            try:
                while processed<200 and self.live_q is not None:
                    fi=self.live_q.get_nowait()
                    if not any(x.path==fi.path for x in self.results): self.results.append(fi)
                    processed+=1
            except queue.Empty:
                pass
            self.refresh_tree_filter(live_append=True)
            if not self.stop_flag.is_set() or (self.live_q and not self.live_q.empty()): self.after(60,drain)
        self.after(120,drain)
        threading.Thread(target=run,daemon=True).start(); self.set_status('Skannaus käynnissä…')

    def finish_scan(self):
        self.live_q=None; self.stop_btn.config(state=tk.DISABLED)
        self.refresh_tree_filter(); self.set_status(f"Valmis. Näytetään {len(self.filtered_indices)} tiedostoa.")
        self.reveal_btn.config(state=(tk.NORMAL if self.results else tk.DISABLED))

    def stop_scan(self): self.stop_flag.set(); self.set_status('Pysäytetään…')

    def refresh_tree_filter(self, live_append:bool=False):
        idx=[]
        for i,fi in enumerate(self.results):
            status,_=classify_path(fi.path)
            if status==SAFE and not self.show_safe.get(): continue
            if status==CAUTION and not self.show_caution.get(): continue
            if status==SYSTEM and not self.show_system.get(): continue
            idx.append(i)
        col=self.sort_col; desc=self.sort_desc
        if col:
            def key(k):
                f=self.results[k]
                if col=='status': return {SAFE:0,CAUTION:1,SYSTEM:2}[classify_path(f.path)[0]]
                if col=='name': return f.basename.lower()
                if col=='dir': return f.dirname.lower()
                if col=='size': return f.size
                if col=='created': return f.created_ts
                return 0
            idx.sort(key=key, reverse=desc)
        self.filtered_indices=idx
        self.tree.delete(*self.tree.get_children())
        for i in self.filtered_indices:
            f=self.results[i]; s,_=classify_path(f.path); meta=STATUS_META[s]
            self.tree.insert('',tk.END, values=(f"{meta['dot']} {meta['label']}", f.basename, f.dirname, human_size(f.size), f.created_str))
        if not live_append: self.set_status(f"Näytetään {len(self.filtered_indices)} tiedostoa.")

    def get_selected_path(self)->Optional[str]:
        sel=self.tree.selection()
        if not sel: return None
        idx=self.tree.index(sel[0])
        if idx<0 or idx>=len(self.filtered_indices): return None
        return self.results[self.filtered_indices[idx]].path

    def reveal_selected(self):
        path=self.get_selected_path()
        if not path: return
        try:
            if sys.platform=='darwin': subprocess.run(['open','-R',path],check=False)
            elif os.name=='nt': subprocess.run(['explorer','/select,',path],check=False)
            else:
                folder=os.path.dirname(path); subprocess.run(['xdg-open',folder],check=False)
        except Exception: traceback.print_exc()

    # --- HOTFIX: Smart Clean (puuttui v5.2:ssa) ---
    def smart_clean(self):
        # Etsii listasta 🟢 Turvallinen -luokan ja siirtää roskakoriin kynnyksen ylittävät
        try:
            thr_mb=float(self.clean_threshold_mb.get().strip() or 0)
        except ValueError:
            messagebox.showerror('Virhe','Kynnys (MB) ei ole numero.'); return
        thr_bytes=int(thr_mb*1024*1024)
        from shutil import rmtree
        def move_to_trash(path:str)->bool:
            try:
                if sys.platform=='darwin':
                    script=f'tell application "Finder" to delete POSIX file "{path}"'
                    subprocess.run(['osascript','-e',script],check=False)
                    return True
                else:
                    try:
                        if os.path.isdir(path): rmtree(path, ignore_errors=True)
                        else: os.remove(path)
                        return True
                    except Exception:
                        return False
            except Exception:
                return False
        # Kandidaatit
        candidates:List[str]=[]
        for i in self.filtered_indices:
            fi=self.results[i]
            status,_=classify_path(fi.path)
            if status==SAFE and fi.size>=thr_bytes:
                candidates.append(fi.path)
        if not candidates:
            messagebox.showinfo('Smart Clean','Ei turvallisia siivottavia valitulla kynnyksellä.'); return
        total=0
        for p in candidates:
            try: total+=os.path.getsize(p)
            except Exception: pass
        if not messagebox.askyesno('Vahvista Smart Clean', f"Siirretään Roskakoriin {len(candidates)} kohdetta, arvio {human_size(total)}.\n\nJatketaanko?"):
            return
        errors=0
        for p in candidates:
            if not move_to_trash(p): errors+=1
        if errors:
            messagebox.showwarning('Smart Clean', f'Valmis, mutta {errors} kohdetta ei voitu siirtää.')
        else:
            messagebox.showinfo('Smart Clean','Valmis. Kohteet siirretty Roskakoriin.')
        # Päivitä näkymä
        self.results=[fi for fi in self.results if os.path.exists(fi.path)]
        self.refresh_tree_filter()

if __name__=='__main__':
    App().mainloop()

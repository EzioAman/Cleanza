import pandas as pd
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
from pandastable import Table
from tkcalendar import DateEntry
import threading
import queue
import re

CHUNK_SIZE = 10000

class ChunkedCSVViewerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("📊 Chunked CSV/Excel Viewer")
        self.root.geometry("1280x700")

        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TButton", font=("Segoe UI", 10), padding=6)
        style.configure("TLabel", font=("Segoe UI", 10))
        style.configure("TEntry", font=("Segoe UI", 10))

        self.file_path = None
        self.df_full = pd.DataFrame()
        self.queue = queue.Queue()
        self.sheet_selected = None
        self.chunk_loader_thread = None
        self.to_delete_df = pd.DataFrame()

        self.create_widgets()

    def create_widgets(self):
        top_row = ttk.Frame(self.root)
        top_row.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)

        self.btn_load = ttk.Button(top_row, text="📂 Load File", command=self.load_file)
        self.btn_load.pack(side=tk.LEFT, padx=5)

        self.btn_preview = ttk.Button(top_row, text="👁 Preview (20)", command=self.show_preview, state=tk.DISABLED)
        self.btn_preview.pack(side=tk.LEFT, padx=5)

        self.btn_show_all = ttk.Button(top_row, text="📄 Show All", command=self.show_full_data, state=tk.DISABLED)
        self.btn_show_all.pack(side=tk.LEFT, padx=5)

        self.btn_export = ttk.Button(top_row, text="💾 Export", command=self.export_to_excel, state=tk.DISABLED)
        self.btn_export.pack(side=tk.LEFT, padx=5)

        self.btn_confirm_delete = ttk.Button(top_row, text="🗑 Confirm Delete", command=self.confirm_deletion, state=tk.DISABLED)
        self.btn_confirm_delete.pack(side=tk.LEFT, padx=5)

        self.btn_search = ttk.Button(top_row, text="🔍 Search", command=self.search_field, state=tk.DISABLED)
        self.btn_search.pack(side=tk.LEFT, padx=10)

        self.progress = ttk.Progressbar(top_row, orient="horizontal", length=200, mode="determinate")
        self.progress.pack(side=tk.LEFT, padx=10)

        self.status = ttk.Label(top_row, text="Status: Idle")
        self.status.pack(side=tk.LEFT, padx=5)

        filter_row = ttk.Frame(self.root)
        filter_row.pack(side=tk.TOP, fill=tk.X, padx=10, pady=5)

        self.btn_filter_plan = ttk.Button(filter_row, text="🧩 PLAN", command=lambda: self.filter_column("PLAN"), state=tk.DISABLED)
        self.btn_filter_plan.pack(side=tk.LEFT, padx=5)

        self.btn_filter_program = ttk.Button(filter_row, text="📝 PROGRAM", command=lambda: self.filter_column("PROGRAM"), state=tk.DISABLED)
        self.btn_filter_program.pack(side=tk.LEFT, padx=5)

        self.btn_filter_followup = ttk.Button(filter_row, text="📌 FOLLOWUP", command=lambda: self.filter_column("FOLLOWUP"), state=tk.DISABLED)
        self.btn_filter_followup.pack(side=tk.LEFT, padx=5)

        self.btn_filter_academic = ttk.Button(filter_row, text="🎓 ACADEMIC", command=lambda: self.filter_column("ACADEMIC CAREER"), state=tk.DISABLED)
        self.btn_filter_academic.pack(side=tk.LEFT, padx=5)

        self.btn_filter_created = ttk.Button(filter_row, text="📅 CREATED DATE", command=self.filter_created_date, state=tk.DISABLED)
        self.btn_filter_created.pack(side=tk.LEFT, padx=5)

        self.btn_summary = ttk.Button(filter_row, text="📊 Lead Summary", command=self.open_lead_summary_flow, state=tk.DISABLED)
        self.btn_summary.pack(side=tk.LEFT, padx=5)

        self.table_frame = ttk.Frame(self.root)
        self.table_frame.pack(fill=tk.BOTH, expand=1)

        log_frame = ttk.Frame(self.root)
        log_frame.pack(fill=tk.BOTH, padx=10, pady=10)

        self.log_scroll_y = ttk.Scrollbar(log_frame)
        self.log_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_text = tk.Text(log_frame, height=8, wrap=tk.NONE, yscrollcommand=self.log_scroll_y.set, font=("Consolas", 10))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_scroll_y.config(command=self.log_text.yview)

    def log(self, message):
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)

    def load_file(self):
        self.file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx *.xls"), ("CSV files", "*.csv")])
        if not self.file_path:
            return

        self.df_full = pd.DataFrame()
        self.sheet_selected = None
        self.to_delete_df = pd.DataFrame()
        self.clear_table()
        for btn in [self.btn_preview, self.btn_show_all, self.btn_search, self.btn_filter_plan,
                    self.btn_filter_program, self.btn_filter_followup, self.btn_filter_academic,
                    self.btn_filter_created, self.btn_export, self.btn_confirm_delete]:
            btn.config(state=tk.DISABLED)

        self.progress["value"] = 0
        self.status.config(text="Status: Loading...")
        self.log(f"Loading file: {self.file_path}")

        self.chunk_loader_thread = threading.Thread(target=self.load_chunks)
        self.chunk_loader_thread.start()
        self.root.after(100, self.check_queue)

    def load_chunks(self):
        try:
            if self.file_path.endswith(".csv"):
                total_rows = sum(1 for _ in open(self.file_path)) - 1
                reader = pd.read_csv(self.file_path, chunksize=CHUNK_SIZE)
            else:
                xl = pd.ExcelFile(self.file_path)
                self.sheet_selected = self.ask_sheet(xl.sheet_names)
                if not self.sheet_selected:
                    return
                df = xl.parse(self.sheet_selected)
                total_rows = len(df)
                reader = [df[i:i + CHUNK_SIZE] for i in range(0, total_rows, CHUNK_SIZE)]

            chunks = []
            count = 0
            first_preview_sent = False

            for chunk in reader:
                chunk = chunk.fillna('')
                chunks.append(chunk)
                count += len(chunk)
                self.queue.put(("progress", int((count / total_rows) * 100)))
                if not first_preview_sent and count >= 20:
                    self.queue.put(("preview", chunk.head(20)))
                    first_preview_sent = True

            df_combined = pd.concat(chunks, ignore_index=True)
            self.queue.put(("done", df_combined))

        except Exception as e:
            self.queue.put(("error", str(e)))

    def ask_sheet(self, sheets):
        selected = tk.StringVar(value=sheets[0])
        win = tk.Toplevel(self.root)
        win.title("Select Sheet")
        tk.Label(win, text="Choose a sheet:").pack(pady=5)
        for sheet in sheets:
            tk.Radiobutton(win, text=sheet, variable=selected, value=sheet).pack(anchor=tk.W)
        tk.Button(win, text="OK", command=win.destroy).pack(pady=5)
        win.grab_set()
        self.root.wait_window(win)
        return selected.get()

    def check_queue(self):
        try:
            while not self.queue.empty():
                msg_type, data = self.queue.get_nowait()
                if msg_type == "progress":
                    self.progress["value"] = data
                    self.status.config(text=f"Loading... {data}%")
                elif msg_type == "preview":
                    self.display_table(data)
                    self.btn_preview.config(state=tk.NORMAL)
                    self.log("Preview ready (first 20 rows).")
                elif msg_type == "done":
                    self.df_full = data
                    for btn in [self.btn_show_all, self.btn_search, self.btn_filter_plan, self.btn_filter_program,
                                self.btn_filter_followup, self.btn_filter_academic, self.btn_filter_created,
                                self.btn_export, self.btn_summary]:
                        btn.config(state=tk.NORMAL)
                    self.progress["value"] = 100
                    self.status.config(text="Status: Loaded")
                    self.log("File loaded successfully.")
                elif msg_type == "error":
                    messagebox.showerror("Error", data)
                    self.status.config(text="Status: Error")
                    self.log(f"Error: {data}")
        except Exception as e:
            self.log(f"Queue error: {e}")
        self.root.after(100, self.check_queue)

    def clear_table(self):
        for widget in self.table_frame.winfo_children():
            widget.destroy()

    def show_preview(self):
        if not self.df_full.empty:
            self.display_table(self.df_full.head(20))
            self.log("Showing preview of first 20 rows.")

    def show_full_data(self):
        if not self.df_full.empty:
            self.display_table(self.df_full)
            self.status.config(text="Status: Showing All Rows")
            self.log("Displayed full data.")

    def display_table(self, df):
        self.clear_table()
        pt = Table(self.table_frame, dataframe=df, showtoolbar=True, showstatusbar=True)
        pt.show()
        pt.autoResizeColumns()
        pt.redraw()


    def filter_column(self, column):
        if self.df_full.empty or column not in self.df_full.columns:
            self.log(f"{column} column not found.")
            return

        top = tk.Toplevel(self.root)
        top.title(f"Filter: {column}")

        canvas = tk.Canvas(top)
        scrollbar = tk.Scrollbar(top, orient="vertical", command=canvas.yview)
        scroll_frame = tk.Frame(canvas)
        scroll_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        values = sorted(self.df_full[column].dropna().unique())
        var_dict = {}

        for val in values:
            var = tk.BooleanVar()
            chk = tk.Checkbutton(scroll_frame, text=str(val), variable=var)
            chk.pack(anchor="w")
            var_dict[val] = var

        def apply_filter():
            selected = [k for k, v in var_dict.items() if v.get()]
            if selected:
                self.to_delete_df = self.df_full[self.df_full[column].isin(selected)]
                self.display_table(self.to_delete_df)
                self.btn_confirm_delete.config(state=tk.NORMAL)
                self.log(f"{len(self.to_delete_df)} rows selected for deletion from column '{column}'.")
            top.destroy()

        tk.Button(scroll_frame, text="Apply Filter", command=apply_filter).pack(pady=10)

    def filter_created_date(self):
        col = "CREATED DATE (STEP 1)"
        if self.df_full.empty or col not in self.df_full.columns:
            self.log("CREATED DATE column not found.")
            return

        def extract_month_year(val):
            match = re.search(r'\d{1,2} (\w{3}) (\d{4})', str(val))
            return f"{match.group(1)} {match.group(2)}" if match else ""

        self.df_full["_month_year"] = self.df_full[col].apply(extract_month_year)
        values = sorted(self.df_full["_month_year"].dropna().unique())

        top = tk.Toplevel(self.root)
        top.title("Filter: CREATED DATE (Month-Year)")

        canvas = tk.Canvas(top)
        scrollbar = tk.Scrollbar(top, orient="vertical", command=canvas.yview)
        scroll_frame = tk.Frame(canvas)
        scroll_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        var_dict = {}
        for val in values:
            var = tk.BooleanVar()
            chk = tk.Checkbutton(scroll_frame, text=val, variable=var)
            chk.pack(anchor="w")
            var_dict[val] = var

        def apply_filter():
            selected = [k for k, v in var_dict.items() if v.get()]
            if selected:
                self.to_delete_df = self.df_full[self.df_full["_month_year"].isin(selected)]
                self.display_table(self.to_delete_df)
                self.btn_confirm_delete.config(state=tk.NORMAL)
                self.log(f"{len(self.to_delete_df)} rows selected for deletion by date: {', '.join(selected)}")
            top.destroy()

        tk.Button(scroll_frame, text="Apply Filter", command=apply_filter).pack(pady=10)

    def confirm_deletion(self):
        if self.to_delete_df.empty:
            self.log("Nothing selected for deletion.")
            return
        self.df_full.drop(index=self.to_delete_df.index, inplace=True)
        self.df_full.reset_index(drop=True, inplace=True)
        self.to_delete_df = pd.DataFrame()
        self.btn_confirm_delete.config(state=tk.DISABLED)
        self.display_table(self.df_full)
        self.log("Selected rows deleted and data refreshed.")

    def search_field(self):
        if self.df_full.empty:
            self.log("Search not available until data is loaded.")
            return

        top = tk.Toplevel(self.root)
        top.title("Search Field")

        tk.Label(top, text="Select Column:").grid(row=0, column=0, padx=5, pady=5)
        col_var = tk.StringVar(top)
        columns = list(self.df_full.columns)
        if columns:
            col_var.set(columns[0])
        col_menu = ttk.Combobox(top, textvariable=col_var, values=columns, state="readonly")
        col_menu.grid(row=0, column=1, padx=5)

        tk.Label(top, text="Enter Value:").grid(row=1, column=0, padx=5, pady=5)
        val_entry = tk.Entry(top)
        val_entry.grid(row=1, column=1, padx=5)

        match_type = tk.StringVar(value="contains")
        tk.Radiobutton(top, text="Contains", variable=match_type, value="contains").grid(row=2, column=0, sticky="w")
        tk.Radiobutton(top, text="Exact", variable=match_type, value="exact").grid(row=2, column=1, sticky="w")
        tk.Radiobutton(top, text="Startswith", variable=match_type, value="startswith").grid(row=3, column=0, sticky="w")

        def run_search():
            col = col_var.get()
            val = val_entry.get().strip()
            if not val:
                self.log("Search value cannot be empty.")
                return
            if col in self.df_full.columns:
                series = self.df_full[col].astype(str)
                if match_type.get() == "contains":
                    results = self.df_full[series.str.contains(val, na=False, case=False)]
                elif match_type.get() == "exact":
                    results = self.df_full[series == val]
                else:
                    results = pd.DataFrame()
                self.display_table(results)
                self.log(f"Found {len(results)} matching rows for '{val}' in column '{col}'.")
            else:
                self.log("Invalid column selected.")

        tk.Button(top, text="Search", command=run_search).grid(row=4, column=0, columnspan=2, pady=10)

    def export_to_excel(self):
        if self.df_full.empty:
            self.log("No data to export.")
            return
        try:
            export_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
            if export_path:
                self.df_full.to_excel(export_path, index=False)
                self.log(f"Data exported to {export_path}")
        except Exception as e:
            messagebox.showerror("Export Error", str(e))
            self.log(f"Export error: {e}")
            
    def open_lead_summary_flow(self):
        if self.df_full.empty:
            self.log("Data not loaded.")
            return
    
        top = tk.Toplevel(self.root)
        top.title("📊 Lead Summary")
    
        tk.Label(top, text="Start Date (DD-MM-YYYY):").grid(row=0, column=0, padx=5, pady=5)
        start_cal = DateEntry(top, date_pattern='dd-mm-yyyy')
        start_cal.grid(row=0, column=1, padx=5)
    
        tk.Label(top, text="End Date (DD-MM-YYYY):").grid(row=1, column=0, padx=5, pady=5)
        end_cal = DateEntry(top, date_pattern='dd-mm-yyyy')
        end_cal.grid(row=1, column=1, padx=5)
    
        tk.Label(top, text="Group by Columns (Ctrl+Click):").grid(row=2, column=0, padx=5, pady=5)
        valid_cols = [col for col in self.df_full.columns if self.df_full[col].dtype == 'object']
        group_listbox = tk.Listbox(top, selectmode=tk.MULTIPLE, height=16, width=40, exportselection=False)
        for col in valid_cols:
            group_listbox.insert(tk.END, col)
        group_listbox.grid(row=2, column=1, padx=5, pady=5)
    
        def generate_summary():
            selected_indices = group_listbox.curselection()
            group_cols = [group_listbox.get(i) for i in selected_indices]
            if not group_cols:
                messagebox.showerror("Error", "Select at least one group by column.")
                return
    
            try:
                start = pd.to_datetime(start_cal.get(), format="%d-%m-%Y")
                end = pd.to_datetime(end_cal.get(), format="%d-%m-%Y")
                end = end.replace(hour=23, minute=59, second=59)
            except Exception as e:
                messagebox.showerror("Date Error", str(e))
                return
    
            try:
                df = self.df_full.copy()
                df = df[df["STEP COMPLETED"].astype(str).str.strip() != "#0"]
                df["CREATED DATE (STEP 1)"] = pd.to_datetime(df["CREATED DATE (STEP 1)"], errors="coerce")
                df["STEP 3 DATE"] = pd.to_datetime(df["STEP 3 DATE"], errors="coerce")
    
                df_filtered = df[df["CREATED DATE (STEP 1)"].between(start, end)]
                total_leads = df_filtered.groupby(group_cols).size().reset_index(name="Lead")
    
                paid_mask = (
                    (df_filtered["PAYMENT STATUS"].astype(str).str.strip().str.lower() == "paid") &
                    (df_filtered["STEP 3 DATE"].between(start, end))
                )
                paid_leads = df_filtered[paid_mask].groupby(group_cols).size().reset_index(name="Paid Lead")
    
                result = pd.merge(total_leads, paid_leads, on=group_cols, how="left").fillna(0)
                result["Paid Lead"] = result["Paid Lead"].astype(int)
    
                self.display_table(result)
                self.log(f"Summary generated with {len(result)} rows.")
                top.destroy()
            except Exception as e:
                messagebox.showerror("Summary Error", str(e))
                self.log(f"Summary error: {e}")
    
        tk.Button(top, text="Generate Summary", command=generate_summary).grid(row=3, column=0, columnspan=2, pady=10)



if __name__ == "__main__":
    root = tk.Tk()
    app = ChunkedCSVViewerApp(root)
    root.mainloop()
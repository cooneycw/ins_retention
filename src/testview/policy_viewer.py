"""
Policy Viewer - Tkinter GUI for exploring inforce data with major changes.

This module provides a visual debugger that allows cycling through policies
and months to view driver assignments, aging, and major change indicators.
"""

import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import sys
import os

# Add the project root to the path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class PolicyViewer:
    """Tkinter GUI for viewing policy data with major changes."""
    
    def __init__(self, csv_path: str = "data/inforce/inforce_monthly_view_sorted"):
        """
        Initialize the Policy Viewer.
        
        Args:
            csv_path: Path to the inforce_monthly_view.csv file
        """
        self.csv_path = Path(csv_path)
        self.df: Optional[pd.DataFrame] = None
        self.current_policy: Optional[str] = None
        self.current_year: int = 2018
        self.current_month: int = 1
        
        # Policy and time navigation data
        self.policies: List[str] = []
        self.policy_index: int = 0
        self.years_months: List[Tuple[int, int]] = []
        self.time_index: int = 0
        
        # GUI components
        self.root: Optional[tk.Tk] = None
        self.policy_var: Optional[tk.StringVar] = None
        self.time_var: Optional[tk.StringVar] = None
        self.major_change_var: Optional[tk.StringVar] = None
        
        # Data display components
        self.policy_info_frame: Optional[ttk.Frame] = None
        self.driver_assignments_frame: Optional[ttk.Frame] = None
        self.major_changes_frame: Optional[ttk.Frame] = None
        
        self.load_data()
        self.create_gui()
    
    def load_data(self) -> None:
        """Load the inforce data from CSV."""
        try:
            # Handle both directory and file paths
            if self.csv_path.is_dir():
                # Find the CSV file in the directory
                csv_files = list(self.csv_path.glob("part-*.csv"))
                if not csv_files:
                    raise FileNotFoundError(f"No CSV files found in directory: {self.csv_path}")
                
                # Use the file with the most records (usually part-00000)
                import pandas as pd
                best_file = None
                max_records = 0
                for csv_file in csv_files:
                    try:
                        # Quick count of records without loading all data
                        with open(csv_file, 'r') as f:
                            record_count = sum(1 for line in f) - 1  # Subtract header
                        if record_count > max_records:
                            max_records = record_count
                            best_file = csv_file
                    except Exception:
                        continue
                
                csv_file = best_file if best_file else csv_files[0]
            else:
                csv_file = self.csv_path
            
            if not csv_file.exists():
                raise FileNotFoundError(f"CSV file not found: {csv_file}")
            
            print(f"Loading data from {csv_file}...")
            self.df = pd.read_csv(csv_file, dtype={'policy': str})
            
            # Get unique policies and sort them
            self.policies = sorted(self.df['policy'].unique())
            
            # Get unique year-month combinations and sort them
            year_month_combos = self.df[['inforce_yy', 'inforce_mm']].drop_duplicates()
            self.years_months = sorted([(row['inforce_yy'], row['inforce_mm']) 
                                      for _, row in year_month_combos.iterrows()])
            
            print(f"Loaded {len(self.df)} records for {len(self.policies)} policies")
            print(f"Date range: {self.years_months[0]} to {self.years_months[-1]}")
            
            # Set initial values - find first policy with major change
            if self.policies:
                # Look for first policy with a major change
                major_change_policy = self._find_first_major_change_policy()
                if major_change_policy:
                    self.current_policy = major_change_policy
                    print(f"Auto-navigated to policy with major change: {self.current_policy}")
                    
                    # Also find the first month with a major change for this policy
                    first_major_change_month = self._find_first_major_change_month(major_change_policy)
                    if first_major_change_month:
                        self.current_year, self.current_month = first_major_change_month
                        print(f"Auto-navigated to first major change month: {self.current_year}-{self.current_month:02d}")
                else:
                    self.current_policy = self.policies[0]
                    print("No major changes found, using first policy")
            
            # Set initial time if not already set
            if not hasattr(self, 'current_year') and self.years_months:
                self.current_year, self.current_month = self.years_months[0]
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load data: {e}")
            sys.exit(1)
    
    def _find_first_major_change_policy(self) -> str:
        """Find the first policy that has a major change."""
        if 'major_change' not in self.df.columns:
            return None
        
        # Get policies with major changes, sorted by policy number
        major_change_policies = self.df[self.df['major_change'] == True]['policy'].unique()
        if len(major_change_policies) > 0:
            # Sort to get the first policy number with a major change
            sorted_policies = sorted(major_change_policies)
            return sorted_policies[0]
        
        return None
    
    def _find_first_major_change_month(self, policy: str) -> tuple:
        """Find the first month with a major change for a given policy."""
        if 'major_change' not in self.df.columns:
            return None
        
        # Get major change months for this policy, sorted by year and month
        policy_major_changes = self.df[
            (self.df['policy'] == policy) & 
            (self.df['major_change'] == True)
        ][['inforce_yy', 'inforce_mm']].drop_duplicates()
        
        if len(policy_major_changes) > 0:
            # Sort by year and month to get the earliest major change
            sorted_changes = policy_major_changes.sort_values(['inforce_yy', 'inforce_mm'])
            first_change = sorted_changes.iloc[0]
            return (first_change['inforce_yy'], first_change['inforce_mm'])
        
        return None
    
    def create_gui(self) -> None:
        """Create the tkinter GUI."""
        self.root = tk.Tk()
        self.root.title("Policy Viewer - Inforce Data Debugger")
        self.root.geometry("1200x800")
        
        # Create main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # Navigation controls
        self.create_navigation_controls(main_frame)
        
        # Data display area
        self.create_data_display(main_frame)
        
        # Status bar
        self.create_status_bar(main_frame)
        
        # Next major change section
        self.create_next_major_change_section(main_frame)
        
        # Load initial data
        self.update_display()
    
    def create_navigation_controls(self, parent: ttk.Frame) -> None:
        """Create navigation controls for policy and time."""
        nav_frame = ttk.LabelFrame(parent, text="Navigation", padding="5")
        nav_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Policy navigation
        policy_frame = ttk.Frame(nav_frame)
        policy_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 20))
        
        ttk.Label(policy_frame, text="Policy:").grid(row=0, column=0, sticky=tk.W)
        
        self.policy_var = tk.StringVar()
        policy_entry = ttk.Entry(policy_frame, textvariable=self.policy_var, width=15)
        policy_entry.grid(row=0, column=1, padx=(5, 5))
        
        ttk.Button(policy_frame, text="← Prev", 
                  command=self.prev_policy).grid(row=0, column=2, padx=2)
        ttk.Button(policy_frame, text="Next →", 
                  command=self.next_policy).grid(row=0, column=3, padx=2)
        
        # Time navigation
        time_frame = ttk.Frame(nav_frame)
        time_frame.grid(row=0, column=1, sticky=(tk.W, tk.E))
        
        ttk.Label(time_frame, text="Time:").grid(row=0, column=0, sticky=tk.W)
        
        self.time_var = tk.StringVar()
        time_entry = ttk.Entry(time_frame, textvariable=self.time_var, width=15)
        time_entry.grid(row=0, column=1, padx=(5, 5))
        
        ttk.Button(time_frame, text="← Prev Month", 
                  command=self.prev_month).grid(row=0, column=2, padx=2)
        ttk.Button(time_frame, text="Next Month →", 
                  command=self.next_month).grid(row=0, column=3, padx=2)
        ttk.Button(time_frame, text="Reset to 2018-01", 
                  command=self.reset_to_2018_01).grid(row=0, column=4, padx=2)
        
        # Bind Enter key to update display
        policy_entry.bind('<Return>', lambda e: self.set_policy_from_entry())
        time_entry.bind('<Return>', lambda e: self.set_time_from_entry())
    
    def create_data_display(self, parent: ttk.Frame) -> None:
        """Create the data display area."""
        # Create notebook for tabs
        notebook = ttk.Notebook(parent)
        notebook.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # Policy info tab
        self.policy_info_frame = ttk.Frame(notebook)
        notebook.add(self.policy_info_frame, text="Policy Info")
        
        # Driver assignments tab
        self.driver_assignments_frame = ttk.Frame(notebook)
        notebook.add(self.driver_assignments_frame, text="Driver Assignments")
        
        # Major changes tab
        self.major_changes_frame = ttk.Frame(notebook)
        notebook.add(self.major_changes_frame, text="Major Changes")
        
        # Major change months tab
        self.major_change_months_frame = ttk.Frame(notebook)
        notebook.add(self.major_change_months_frame, text="Change Timeline")
    
    def create_status_bar(self, parent: ttk.Frame) -> None:
        """Create status bar."""
        status_frame = ttk.Frame(parent)
        status_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E))
        
        self.major_change_var = tk.StringVar()
        ttk.Label(status_frame, textvariable=self.major_change_var, 
                 font=("Arial", 10, "bold")).pack(side=tk.LEFT)
        
        # Record count
        self.record_count_var = tk.StringVar()
        ttk.Label(status_frame, textvariable=self.record_count_var).pack(side=tk.RIGHT)
    
    def create_next_major_change_section(self, parent: ttk.Frame) -> None:
        """Create the next major change section at the bottom."""
        self.next_major_change_frame = ttk.LabelFrame(parent, text="Next Major Change", padding="5")
        self.next_major_change_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(5, 0))
        
        # This will be populated by update_display()
        self.next_major_change_content_frame = ttk.Frame(self.next_major_change_frame)
        self.next_major_change_content_frame.pack(fill="x")
    
    def update_display(self) -> None:
        """Update the display with current policy and time data."""
        if not self.df is not None or not self.current_policy:
            return
        
        # Update navigation variables
        self.policy_var.set(self.current_policy)
        self.time_var.set(f"{self.current_year}-{self.current_month:02d}")
        
        # Get current data
        current_data = self.df[
            (self.df['policy'] == self.current_policy) & 
            (self.df['inforce_yy'] == self.current_year) & 
            (self.df['inforce_mm'] == self.current_month)
        ]
        
        # Update status
        record_count = len(current_data)
        self.record_count_var.set(f"Records: {record_count}")
        
        # Check for major changes
        major_changes = current_data['major_change'].any() if 'major_change' in current_data.columns else False
        if major_changes:
            transition_date = f"{self.current_year}-{self.current_month:02d}"
            self.major_change_var.set(f"⚠️ MAJOR CHANGE DETECTED - Transition: {transition_date}")
        else:
            self.major_change_var.set("✅ No Major Changes")
        
        # Update tabs
        self.update_policy_info_tab(current_data)
        self.update_driver_assignments_tab(current_data)
        self.update_major_changes_tab(current_data)
        self.update_major_change_months_tab()
        
        # Update next major change section
        self.update_next_major_change_section()
    
    def update_policy_info_tab(self, data: pd.DataFrame) -> None:
        """Update the policy info tab."""
        # Clear existing content
        for widget in self.policy_info_frame.winfo_children():
            widget.destroy()
        
        if data.empty:
            ttk.Label(self.policy_info_frame, text="No data for this policy/time combination").pack(pady=20)
            return
        
        # Create scrollable frame
        canvas = tk.Canvas(self.policy_info_frame)
        scrollbar = ttk.Scrollbar(self.policy_info_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Policy summary
        policy_info = data.iloc[0]
        
        # Format premium properly
        premium = policy_info.get('premium_paid', 'N/A')
        if premium != 'N/A' and pd.notna(premium):
            premium_str = f"${float(premium):.2f}"
        else:
            premium_str = 'N/A'
        
        info_text = f"""
Policy: {policy_info['policy']}
Policy Expiry: {policy_info.get('policy_expiry_date', 'N/A')}
Client Tenure: {policy_info.get('client_tenure_days', 'N/A')} days
Premium Paid: {premium_str}
Inforce Date: {self.current_year}-{self.current_month:02d}
        """.strip()
        
        ttk.Label(scrollable_frame, text=info_text, font=("Arial", 12)).pack(pady=10, padx=10)
        
        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
    
    def update_driver_assignments_tab(self, data: pd.DataFrame) -> None:
        """Update the driver assignments tab with three horizontal windows."""
        # Clear existing content
        for widget in self.driver_assignments_frame.winfo_children():
            widget.destroy()
        
        if data.empty:
            ttk.Label(self.driver_assignments_frame, text="No data for this policy/time combination").pack(pady=20)
            return
        
        # Create three horizontal panes using PanedWindow
        paned_window = ttk.PanedWindow(self.driver_assignments_frame, orient="vertical")
        paned_window.pack(fill="both", expand=True, padx=5, pady=5)
        
        # TOP PANE: All Drivers
        drivers_frame = ttk.LabelFrame(paned_window, text="All Drivers", padding=5)
        paned_window.add(drivers_frame, weight=1)
        
        # Get unique drivers for the CURRENT MONTH from the filtered 'data'
        unique_drivers = data[['driver_no', 'driver_name', 'driver_age']].drop_duplicates()
        
        drivers_columns = ['driver_no', 'driver_name', 'driver_age']
        drivers_tree = ttk.Treeview(drivers_frame, columns=drivers_columns, show='headings', height=4)
        
        for col in drivers_columns:
            drivers_tree.heading(col, text=col.replace('_', ' ').title())
            drivers_tree.column(col, width=150)
        
        for _, row in unique_drivers.iterrows():
            values = [str(row.get(col, 'N/A')) for col in drivers_columns]
            drivers_tree.insert('', 'end', values=values)
        
        drivers_scrollbar = ttk.Scrollbar(drivers_frame, orient="vertical", command=drivers_tree.yview)
        drivers_tree.configure(yscrollcommand=drivers_scrollbar.set)
        
        drivers_tree.pack(side="left", fill="both", expand=True)
        drivers_scrollbar.pack(side="right", fill="y")
        
        # MIDDLE PANE: Driver/Vehicle Assignments
        assignments_frame = ttk.LabelFrame(paned_window, text="Driver/Vehicle Assignments", padding=5)
        paned_window.add(assignments_frame, weight=2)
        
        assignments_columns = ['vehicle_no', 'driver_no', 'driver_name', 'driver_age', 'assignment_type', 'exposure_factor']
        assignments_tree = ttk.Treeview(assignments_frame, columns=assignments_columns, show='headings', height=6)
        
        for col in assignments_columns:
            assignments_tree.heading(col, text=col.replace('_', ' ').title())
            assignments_tree.column(col, width=120)
        
        for _, row in data.iterrows():
            values = [str(row.get(col, 'N/A')) for col in assignments_columns]
            assignments_tree.insert('', 'end', values=values)
        
        assignments_scrollbar = ttk.Scrollbar(assignments_frame, orient="vertical", command=assignments_tree.yview)
        assignments_tree.configure(yscrollcommand=assignments_scrollbar.set)
        
        assignments_tree.pack(side="left", fill="both", expand=True)
        assignments_scrollbar.pack(side="right", fill="y")
        
        # BOTTOM PANE: All Vehicles
        vehicles_frame = ttk.LabelFrame(paned_window, text="All Vehicles", padding=5)
        paned_window.add(vehicles_frame, weight=1)
        
        # Get unique vehicles for this policy
        unique_vehicles = data[['vehicle_no', 'vehicle_type', 'model_year']].drop_duplicates()
        
        vehicles_columns = ['vehicle_no', 'vehicle_type', 'model_year']
        vehicles_tree = ttk.Treeview(vehicles_frame, columns=vehicles_columns, show='headings', height=4)
        
        for col in vehicles_columns:
            vehicles_tree.heading(col, text=col.replace('_', ' ').title())
            vehicles_tree.column(col, width=150)
        
        for _, row in unique_vehicles.iterrows():
            values = [str(row.get(col, 'N/A')) for col in vehicles_columns]
            vehicles_tree.insert('', 'end', values=values)
        
        vehicles_scrollbar = ttk.Scrollbar(vehicles_frame, orient="vertical", command=vehicles_tree.yview)
        vehicles_tree.configure(yscrollcommand=vehicles_scrollbar.set)
        
        vehicles_tree.pack(side="left", fill="both", expand=True)
        vehicles_scrollbar.pack(side="right", fill="y")
    
    def update_major_changes_tab(self, data: pd.DataFrame) -> None:
        """Update the major changes tab to show all major changes for current policy."""
        # Clear existing content
        for widget in self.major_changes_frame.winfo_children():
            widget.destroy()
        
        if not self.current_policy:
            ttk.Label(self.major_changes_frame, text="No policy selected").pack(pady=20)
            return
        
        # Check if major_change column exists
        if 'major_change' not in self.df.columns:
            ttk.Label(self.major_changes_frame, text="Major change data not available").pack(pady=20)
            return
        
        # Get ALL major changes for the current policy (not just current time)
        policy_major_changes = self.df[
            (self.df['policy'] == self.current_policy) & 
            (self.df['major_change'] == True)
        ]
        
        if policy_major_changes.empty:
            ttk.Label(self.major_changes_frame, text="✅ No Major Changes for This Policy", 
                     font=("Arial", 14, "bold"), foreground="green").pack(pady=20)
            return
        
        # Show current time status
        current_time_data = data[data['major_change'] == True] if not data.empty else pd.DataFrame()
        current_time_has_major_change = not current_time_data.empty
        
        if current_time_has_major_change:
            transition_date = f"{self.current_year}-{self.current_month:02d}"
            ttk.Label(self.major_changes_frame, text="⚠️ MAJOR CHANGE AT CURRENT TIME", 
                     font=("Arial", 14, "bold"), foreground="red").pack(pady=5)
            ttk.Label(self.major_changes_frame, text=f"Current Time: {transition_date}", 
                     font=("Arial", 12, "bold"), foreground="blue").pack(pady=5)
        else:
            ttk.Label(self.major_changes_frame, text="📅 No Major Change at Current Time", 
                     font=("Arial", 12, "bold"), foreground="orange").pack(pady=5)
            ttk.Label(self.major_changes_frame, text=f"Current Time: {self.current_year}-{self.current_month:02d}", 
                     font=("Arial", 10)).pack(pady=2)
        
        # Show all major changes for this policy
        ttk.Label(self.major_changes_frame, text="All Major Changes for This Policy:", 
                 font=("Arial", 12, "bold")).pack(pady=(10, 5))
        
        # Create treeview for all major change records
        columns = ['inforce_yy', 'inforce_mm', 'vehicle_no', 'driver_no', 'driver_name', 'assignment_type']
        tree = ttk.Treeview(self.major_changes_frame, columns=columns, show='headings', height=8)
        
        # Define better column headers and widths
        column_config = {
            'inforce_yy': ('Year', 60),
            'inforce_mm': ('Month', 60),
            'vehicle_no': ('Vehicle', 80),
            'driver_no': ('Driver #', 80),
            'driver_name': ('Driver Name', 150),
            'assignment_type': ('Assignment', 120)
        }
        
        for col in columns:
            header, width = column_config.get(col, (col.replace('_', ' ').title(), 120))
            tree.heading(col, text=header)
            tree.column(col, width=width)
        
        # Sort by time and add records
        sorted_changes = policy_major_changes.sort_values(['inforce_yy', 'inforce_mm', 'vehicle_no', 'driver_no'])
        for _, row in sorted_changes.iterrows():
            values = [str(row.get(col, 'N/A')) for col in columns]
            tree.insert('', 'end', values=values)
        
        tree.pack(pady=10, padx=10, fill="both", expand=True)
        
        # Show next major change for this policy
        self._show_next_major_change_for_policy(policy_major_changes)
    
    def _show_next_major_change_for_policy(self, policy_major_changes: pd.DataFrame) -> None:
        """Show the next major change for the current policy after the current time."""
        # Get unique time periods for major changes, sorted
        time_periods = policy_major_changes[['inforce_yy', 'inforce_mm']].drop_duplicates().sort_values(['inforce_yy', 'inforce_mm'])
        
        # Find the next major change after current time
        next_major_change = None
        for _, row in time_periods.iterrows():
            if (row['inforce_yy'] > self.current_year) or \
               (row['inforce_yy'] == self.current_year and row['inforce_mm'] > self.current_month):
                next_major_change = row
                break
        
        if next_major_change is not None:
            # Create frame for next major change info
            next_frame = ttk.LabelFrame(self.major_changes_frame, text="Next Major Change for This Policy", padding=10)
            next_frame.pack(pady=10, padx=10, fill="x")
            
            next_date = f"{next_major_change['inforce_yy']}-{next_major_change['inforce_mm']:02d}"
            ttk.Label(next_frame, text=f"Next Major Change: {next_date}", 
                     font=("Arial", 12, "bold"), foreground="blue").pack(pady=2)
            
            # Just show the information without automatic navigation
            ttk.Label(next_frame, text="💡 Use the time navigation buttons to manually navigate to this date", 
                     font=("Arial", 9), foreground="gray").pack(pady=2)
        else:
            # No more major changes for this policy
            ttk.Label(self.major_changes_frame, text="🎉 No more major changes remaining for this policy", 
                     font=("Arial", 10, "bold"), foreground="green").pack(pady=10)
    
    def _jump_to_time(self, year: int, month: int) -> None:
        """Jump to a specific time period."""
        self.current_year = year
        self.current_month = month
        self.update_display()
        print(f"Jumped to {year}-{month:02d}")
    
    def update_next_major_change_section(self) -> None:
        """Update the next major change section at the bottom of the window."""
        # Clear existing content
        for widget in self.next_major_change_content_frame.winfo_children():
            widget.destroy()
        
        # Find next policy with major change
        next_major_change = self._find_next_major_change_policy()
        
        if next_major_change:
            # Show next policy info
            policy_info = f"Policy: {next_major_change['policy']}"
            month_info = f"First Major Change: {next_major_change['year']}-{next_major_change['month']:02d}"
            
            ttk.Label(self.next_major_change_content_frame, text=policy_info, font=("Arial", 12, "bold")).pack(pady=2)
            ttk.Label(self.next_major_change_content_frame, text=month_info, font=("Arial", 10)).pack(pady=2)
            
            # Navigation button
            nav_button = ttk.Button(self.next_major_change_content_frame, text="Navigate to Policy", 
                                  command=lambda: self._navigate_to_policy(next_major_change['policy']))
            nav_button.pack(pady=5)
        else:
            # No more major changes found
            ttk.Label(self.next_major_change_content_frame, text="🎉 No more major changes found in dataset", 
                     font=("Arial", 10), foreground="blue").pack(pady=10)
    
    def _find_next_major_change_policy(self) -> dict:
        """Find the next policy with a major change after the current policy."""
        if 'major_change' not in self.df.columns:
            return None
        
        # Get all policies with major changes, sorted by policy number
        major_change_policies = self.df[self.df['major_change'] == True]['policy'].unique()
        sorted_policies = sorted(major_change_policies)
        
        # Find current policy index
        try:
            current_index = sorted_policies.index(self.current_policy)
            # Get next policy
            if current_index + 1 < len(sorted_policies):
                next_policy = sorted_policies[current_index + 1]
                
                # Find first major change month for this policy
                first_major_change_month = self._find_first_major_change_month(next_policy)
                if first_major_change_month:
                    return {
                        'policy': next_policy,
                        'year': first_major_change_month[0],
                        'month': first_major_change_month[1]
                    }
        except ValueError:
            # Current policy not in major change list, find first one
            if sorted_policies:
                next_policy = sorted_policies[0]
                first_major_change_month = self._find_first_major_change_month(next_policy)
                if first_major_change_month:
                    return {
                        'policy': next_policy,
                        'year': first_major_change_month[0],
                        'month': first_major_change_month[1]
                    }
        
        return None
    
    def _navigate_to_policy(self, policy: str) -> None:
        """Navigate to a specific policy while keeping the current time."""
        # Set the policy and sync index so Prev/Next work predictably
        self.current_policy = policy
        if hasattr(self, 'policies') and self.policies and policy in self.policies:
            self.policy_index = self.policies.index(policy)
        
        # Keep the current time (don't change year/month) and refresh
        self.update_display()
        print(f"Navigated to policy {policy} at current time {self.current_year}-{self.current_month:02d}")
    
    def _add_change_context(self, major_change_records: pd.DataFrame) -> None:
        """Add context about what changed in the major change."""
        if major_change_records.empty:
            return
        
        # Get current policy data
        current_policy = self.current_policy
        current_year = self.current_year
        current_month = self.current_month
        
        if not current_policy or self.df is None:
            return
        
        # Get previous month's data for comparison
        prev_month = current_month - 1
        prev_year = current_year
        if prev_month == 0:
            prev_month = 12
            prev_year = current_year - 1
        
        # Check if previous month exists
        prev_data = self.df[
            (self.df['policy'] == current_policy) & 
            (self.df['inforce_yy'] == prev_year) & 
            (self.df['inforce_mm'] == prev_month)
        ]
        
        if prev_data.empty:
            ttk.Label(self.major_changes_frame, text="⚠️ Cannot compare with previous month (no data)", 
                     font=("Arial", 10), foreground="orange").pack(pady=5)
            return
        
        # Compare current vs previous assignments
        current_assignments = set()
        prev_assignments = set()
        
        for _, row in major_change_records.iterrows():
            current_assignments.add(f"V{row['vehicle_no']}-D{row['driver_no']}")
        
        for _, row in prev_data.iterrows():
            prev_assignments.add(f"V{row['vehicle_no']}-D{row['driver_no']}")
        
        # Determine what changed
        added_assignments = current_assignments - prev_assignments
        removed_assignments = prev_assignments - current_assignments
        
        # Create context frame
        context_frame = ttk.Frame(self.major_changes_frame)
        context_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(context_frame, text="Change Summary:", 
                 font=("Arial", 10, "bold")).pack(anchor="w")
        
        if added_assignments:
            ttk.Label(context_frame, text=f"➕ Added: {', '.join(sorted(added_assignments))}", 
                     font=("Arial", 9), foreground="green").pack(anchor="w")
        
        if removed_assignments:
            ttk.Label(context_frame, text=f"➖ Removed: {', '.join(sorted(removed_assignments))}", 
                     font=("Arial", 9), foreground="red").pack(anchor="w")
        
        if not added_assignments and not removed_assignments:
            ttk.Label(context_frame, text="🔄 Assignment changes detected", 
                     font=("Arial", 9), foreground="blue").pack(anchor="w")
    
    def update_major_change_months_tab(self) -> None:
        """Update the major change months timeline tab."""
        # Clear existing content
        for widget in self.major_change_months_frame.winfo_children():
            widget.destroy()
        
        if not self.current_policy or self.df is None:
            ttk.Label(self.major_change_months_frame, text="No policy selected").pack(pady=20)
            return
        
        # Get all data for current policy
        policy_data = self.df[self.df['policy'] == self.current_policy]
        
        if policy_data.empty:
            ttk.Label(self.major_change_months_frame, text="No data for this policy").pack(pady=20)
            return
        
        # Check if major_change column exists
        if 'major_change' not in policy_data.columns:
            ttk.Label(self.major_change_months_frame, text="Major change data not available").pack(pady=20)
            return
        
        # Get unique months with major changes
        major_change_months = policy_data[policy_data['major_change'] == True][['inforce_yy', 'inforce_mm']].drop_duplicates()
        
        if major_change_months.empty:
            ttk.Label(self.major_change_months_frame, text="✅ No major changes found for this policy", 
                     font=("Arial", 12, "bold"), foreground="green").pack(pady=20)
            return
        
        # Create header
        ttk.Label(self.major_change_months_frame, text="Major Change Timeline", 
                 font=("Arial", 14, "bold")).pack(pady=10)
        
        ttk.Label(self.major_change_months_frame, 
                 text=f"Policy {self.current_policy} has major changes in {len(major_change_months)} month(s):",
                 font=("Arial", 10)).pack(pady=5)
        
        # Create scrollable frame for the timeline
        canvas = tk.Canvas(self.major_change_months_frame)
        scrollbar = ttk.Scrollbar(self.major_change_months_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Sort months chronologically
        major_change_months = major_change_months.sort_values(['inforce_yy', 'inforce_mm'])
        
        # Create clickable buttons for each major change month
        for _, row in major_change_months.iterrows():
            year = row['inforce_yy']
            month = row['inforce_mm']
            
            # Create button frame
            button_frame = ttk.Frame(scrollable_frame)
            button_frame.pack(fill="x", padx=10, pady=2)
            
            # Create clickable button
            button_text = f"{year}-{month:02d}"
            button = ttk.Button(button_frame, text=button_text, 
                              command=lambda y=year, m=month: self.jump_to_month(y, m))
            button.pack(side="left", padx=5)
            
            # Add current indicator
            if year == self.current_year and month == self.current_month:
                ttk.Label(button_frame, text="← CURRENT", 
                         font=("Arial", 8, "bold"), foreground="blue").pack(side="left", padx=5)
        
        # Pack canvas and scrollbar
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
    
    # Navigation methods
    def prev_policy(self) -> None:
        """Go to previous policy."""
        if self.policy_index > 0:
            self.policy_index -= 1
            self.current_policy = self.policies[self.policy_index]
            self.update_display()
    
    def next_policy(self) -> None:
        """Go to next policy."""
        if self.policy_index < len(self.policies) - 1:
            self.policy_index += 1
            self.current_policy = self.policies[self.policy_index]
            self.update_display()
    
    def prev_month(self) -> None:
        """Go to previous month."""
        if self.time_index > 0:
            self.time_index -= 1
            self.current_year, self.current_month = self.years_months[self.time_index]
            self.update_display()
    
    def next_month(self) -> None:
        """Go to next month."""
        if self.time_index < len(self.years_months) - 1:
            self.time_index += 1
            self.current_year, self.current_month = self.years_months[self.time_index]
            self.update_display()
    
    def reset_to_2018_01(self) -> None:
        """Reset time to 2018-01."""
        self.current_year = 2018
        self.current_month = 1
        
        # Find the index for 2018-01 in the years_months list
        target_index = 0
        for i, (year, month) in enumerate(self.years_months):
            if year == 2018 and month == 1:
                target_index = i
                break
        
        self.time_index = target_index
        self.update_display()
    
    def set_policy_from_entry(self) -> None:
        """Set policy from entry field."""
        policy_input = self.policy_var.get().strip()
        if policy_input in self.policies:
            self.current_policy = policy_input
            self.policy_index = self.policies.index(policy_input)
            self.update_display()
        else:
            messagebox.showerror("Error", f"Policy {policy_input} not found")
    
    def set_time_from_entry(self) -> None:
        """Set time from entry field."""
        time_input = self.time_var.get().strip()
        try:
            if '-' in time_input:
                year, month = map(int, time_input.split('-'))
                if (year, month) in self.years_months:
                    self.current_year, self.current_month = year, month
                    self.time_index = self.years_months.index((year, month))
                    self.update_display()
                else:
                    messagebox.showerror("Error", f"Time {time_input} not found in data")
            else:
                messagebox.showerror("Error", "Please use format YYYY-MM")
        except ValueError:
            messagebox.showerror("Error", "Invalid time format. Use YYYY-MM")
    
    def jump_to_month(self, year: int, month: int) -> None:
        """Jump to a specific month (used by major change timeline buttons)."""
        if (year, month) in self.years_months:
            self.current_year, self.current_month = year, month
            self.time_index = self.years_months.index((year, month))
            self.update_display()
        else:
            messagebox.showerror("Error", f"Time {year}-{month:02d} not found in data")
    
    def run(self) -> None:
        """Run the GUI application."""
        if self.root:
            self.root.mainloop()


def main():
    """Main entry point for the Policy Viewer."""
    import argparse
    
    # Check conda environment
    conda_env = os.environ.get('CONDA_DEFAULT_ENV', '')
    if conda_env != 'retention':
        print("⚠️  WARNING: Not running in 'retention' conda environment!")
        print("   Please run: conda activate retention")
        print(f"   Current environment: {conda_env if conda_env else 'none'}")
        print()
    
    parser = argparse.ArgumentParser(description="Policy Viewer - Visual Debugger for Inforce Data")
    parser.add_argument("--csv", default="data/inforce/inforce_monthly_view.csv",
                       help="Path to inforce_monthly_view.csv file")
    
    args = parser.parse_args()
    
    try:
        viewer = PolicyViewer(args.csv)
        viewer.run()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

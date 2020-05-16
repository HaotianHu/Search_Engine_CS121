from tkinter import *
import Search


class MainWindow(Frame):
    def __init__(self,*args, **kwargs):
        Frame.__init__(self, *args, **kwargs)

        self.greetings = Label(self, text = "Hello, welcome to Search Engine!",font = ('Comic Sans MS', 20))
        self.greetings.grid(row = 0, column = 0)
        
        self.query_prompt = Label(self, text = "Enter a query to search: ")
        self.query_prompt.grid(row = 1, column = 1)

        self.search_num = Label(self, text = "Enter the number of results want to display: ")
        self.search_num.grid(row = 2, column = 1)

        self.search_entry = Entry(self)
        self.search_entry.grid(row = 1, column = 2)

        self.num_entry = Entry(self)
        self.num_entry.grid(row = 2, column = 2)

        self.confirm = Button(self,text="confirm to next step",command = self.create) #command = create)
        self.confirm.grid(row = 3, column = 2)

        self.cancel = Button(self,text = "cancel",command = self.quit_window)
        self.cancel.grid(row = 3, column = 1)

    def quit_window(self):
        self.destroy()
        self.quit()

    def searching(self,q,n):
        search = Search.Search("CS121_Index", "HTML_Corpus_Index")
        res_url = search.query(q.lower(),int(n))
        l = search.result_list(res_url)
        return l

    def search_res(self,l):
        t = Toplevel(self)
        t.wm_title("Search result")
        t.wm_geometry("1000x1000")
        scrollbar = Scrollbar(t)
        scrollbar.pack(side=RIGHT, fill=Y)
        listbox = Listbox(t)
        listbox.pack(fill = BOTH)
        listbox.insert(END,"Search Result")
        for i in l:
            listbox.insert(END,i)
        listbox.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=listbox.yview)

    def create(self):
        q = self.search_entry.get()
        n = self.num_entry.get()
        l = self.searching(q,n)
        self.search_res(l)

if __name__ == "__main__":
    root = Tk()
    main = MainWindow(root)
    main.pack(side="top", fill="both", expand=True)
    root.mainloop()

    
    
    
    
    

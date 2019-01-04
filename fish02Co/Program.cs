using System;
// using System.Collections.Generic;
using System.Linq;
using System.Text;
// using System.Threading.Tasks;
using System.Data.SQLite;

// using System.Text.RegularExpressions;
using System.IO;

namespace fish02Co
{
    class Program
    {
        struct ConEgg
        {
            public SQLiteConnection SQLiteCon;
            public SQLiteCommand SQLComm;
            public bool todoOK;
        }

        static void Main(string[] args)
        {
            // 1 parameter create (in order to create de DB).
            // 2 parameter db
            // 3 parameter src file
            // 4 parameter tgt file
            // 5 parameter start line
            // 6 parameter en line
            String[] arguments = Environment.GetCommandLineArgs();
            string[] kk = { "a", "b", "c", "d", "e" };
            string DoICreateDB;
            string DBname;
            string srcfile;
            string tgtfile;
            int startline;
            int endline;
            //
            DoICreateDB = arguments[1];
            DBname = arguments[2];
            srcfile = arguments[3];
            tgtfile = arguments[4];
            startline = Convert.ToInt32(arguments[5]);
            endline = Convert.ToInt32(arguments[6]);

            // srcfile = (@"U:\tmp\fisher\DOGC_VERIFY_BK.ca");
            // tgtfile = (@"U:\tmp\fisher\DOGC_VERIFY_BK.es");


            StreamReader srsrc = new StreamReader(srcfile, Encoding.UTF8, true); // input file
            StreamReader srtgt = new StreamReader(tgtfile, Encoding.UTF8, true); // input file

            ConEgg CE;
            // DBname = @"U:\tmp\fisher\laika.db";
            if (DoICreateDB == "create") { CreateDB(DBname); }

            try
            {
                int nlines = 0;
                CE = OpenDB(DBname); //CE has connection an command.
                using (srsrc)
                {
                    using (srtgt)
                    {
                        while (srsrc.Peek() != -1)
                        {

                            // Read the streams
                            String linesrc = srsrc.ReadLine();
                            String linetgt = srtgt.ReadLine();
                            var SSsrc = new fishCoCommon.SentenceSpliterClass(linesrc, true);
                            var SStgt = new fishCoCommon.SentenceSpliterClass(linetgt, true);
                            nlines += 1;
                            if (nlines > endline) { break; } // end 
                            if (nlines >= startline)
                            {  // process only if nline is >= startline
                                Console.Write(nlines);
                                SSsrc.Split(); SStgt.Split();
                                //linesrc = linesrc.Trim();
                                //linetgt = linetgt.Trim();
                                if (SSsrc.lensentence > 0 & SStgt.lensentence > 0)
                                { // we need a line

                                    foreach (string s in SSsrc.words) { AddKey2DB(CE, s, "src"); }
                                    foreach (string s in SStgt.words) { AddKey2DB(CE, s, "tgt"); }
                                    AddRelations2DB(CE, SSsrc.words, SStgt.words);
                                }
                            }
                        }
                        Console.WriteLine(string.Format("File {0} -> {1} lines", srcfile, nlines));
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Fatal error.");
                Console.WriteLine(String.Format("The file {0} could not be read:", srcfile));
                Console.WriteLine(e.Message);
                return;
            }
            CloseDB(CE);
            Console.WriteLine("The End");
            // Console.ReadLine();




        }
        static bool CreateDB(string DBname)
        {
            File.Delete(DBname); // first delete the file
            SQLiteConnection.CreateFile(DBname); // Create DB
            var DB = new MyDataBase(DBname);
            SQLiteConnection m_dbConnection = new SQLiteConnection(string.Format(
                "Data Source={0};Version=3;", DB.DBname));
            m_dbConnection.Open();
            SQLiteCommand command = new SQLiteCommand(m_dbConnection);

            // string sql = "create table highscores (name varchar(20), score int)";
            string sql;
            sql = "CREATE TABLE 'src' ( `indext` INTEGER NOT NULL UNIQUE, `keyword` TEXT ( 50 ) NOT NULL UNIQUE," +
                " `keywordfreq` INTEGER NOT NULL, " +
                "`freqpercentage` NUMERIC, `freqmargin` NUMERIC, PRIMARY KEY(`indext`) )";
            command.CommandText = sql;
            command.ExecuteNonQuery();
            sql = "CREATE TABLE 'tgt' ( `indext` INTEGER NOT NULL UNIQUE, `keyword` TEXT ( 50 ) NOT NULL UNIQUE," +
                " `keywordfreq` INTEGER NOT NULL, " +
                "`freqpercentage` NUMERIC, `freqmargin` NUMERIC, PRIMARY KEY(`indext`) )";
            command.CommandText = sql;
            command.ExecuteNonQuery();
            sql = "CREATE TABLE `srctgt` ( `indexsrc` INTEGER NOT NULL, `indextgt` INTEGER NOT NULL, `freq` INTEGER, PRIMARY KEY(`indexsrc`,`indextgt`) )";
            command.CommandText = sql;
            command.ExecuteNonQuery();
            sql = "CREATE INDEX `idx_srctgt_src` ON `srctgt` ( `indexsrc` )";
            command.CommandText = sql;
            command.ExecuteNonQuery();
            sql = "CREATE INDEX `idx_srctgt_tgt` ON `srctgt` ( `indextgt` )";
            command.CommandText = sql;
            command.ExecuteNonQuery();

            sql = "CREATE INDEX `idx_src_keyword` ON `src` ( `keyword` )";
            command.CommandText = sql;
            command.ExecuteNonQuery();
            sql = "CREATE INDEX `idx_tgt_keyword` ON `tgt` ( `keyword` )";
            command.CommandText = sql;
            command.ExecuteNonQuery();
            sql = "CREATE TABLE `srccand` ( " +
            "	`indexsrc`	NUMERIC NOT NULL, " +
            "	`indextgt`	NUMERIC NOT NULL, " +
            "	`freqpercentagesample`	NUMERIC, " +
            "	`freqmarginsample`	NUMERIC, " +
            "	`populationsampleratio`	NUMERIC, " +
            "	`freqinsample`	NUMERIC, " +
            "	PRIMARY KEY(`indexsrc`,`indextgt`) " +
            " );";
            command.CommandText = sql;
            command.ExecuteNonQuery();
            sql = "CREATE TABLE `tgtcand` ( " +
            "	`indexsrc`	NUMERIC NOT NULL, " +
            "	`indextgt`	NUMERIC NOT NULL, " +
            "	`freqpercentagesample`	NUMERIC, " +
            "	`freqmarginsample`	NUMERIC, " +
            "	`populationsampleratio`	NUMERIC, " +
            "	`freqinsample`	NUMERIC, " +
            "	PRIMARY KEY(`indexsrc`,`indextgt`) " +
            " );";
            command.CommandText = sql;
            command.ExecuteNonQuery();



            m_dbConnection.Close();


            return true;
        }

        static ConEgg OpenDB(string DBname)
        {
            ConEgg ce;
            var DB = new MyDataBase(DBname);
            // DB.DBCreation();
            SQLiteConnection m_dbConnection = new SQLiteConnection(string.Format(
                "Data Source={0};Version=3;", DB.DBname));
            m_dbConnection.Open();
            // SQLiteCommand command = new SQLiteCommand(m_dbConnection);
            //  begin transaction
            SQLiteCommand command = new SQLiteCommand("begin", m_dbConnection);
            command.ExecuteNonQuery();



            ce.todoOK = true;
            ce.SQLiteCon = m_dbConnection;
            ce.SQLComm = command;

            return ce;
        }
        static void CloseDB(ConEgg ce)
        {
            ce.SQLComm = new SQLiteCommand("end", ce.SQLiteCon);
            ce.SQLComm.ExecuteNonQuery();
            ce.SQLiteCon.Close();
            return;
        }
        static bool AddRelations2DB(ConEgg ce, string[] wordssrc, string[] wordstgt)
        {
            string sqlsrcaux = "select indext from src where keyword='{0}'";
            string sqltgtaux = "select indext from tgt where keyword='{0}'";
            string sql;
            int indexsrc, indextgt;
            object result = "";


            // create all relations
            foreach (string srcstring in wordssrc)
            {
                sql = string.Format(sqlsrcaux, srcstring);
                ce.SQLComm.CommandText = sql;
                indexsrc = Convert.ToInt32(ce.SQLComm.ExecuteScalar());
                foreach (string tgtstring in wordstgt)
                {
                    sql = string.Format(sqltgtaux, tgtstring);
                    ce.SQLComm.CommandText = sql;
                    indextgt = Convert.ToInt32(ce.SQLComm.ExecuteScalar());
                    // have source and target first try to find out if the pair
                    // exists (update) otherwise insert
                    sql = string.Format("select freq from srctgt where indexsrc={0} and indextgt={1}",
                        indexsrc, indextgt);
                    ce.SQLComm.CommandText = sql;
                    result = ce.SQLComm.ExecuteScalar();
                    if (ce.SQLComm.ExecuteScalar() != null)
                    {
                        // ya existe solo tengo que actualizar
                        //Console.Write(" ");
                        sql = string.Format("update srctgt set freq={0} where indexsrc={1} and indextgt={2}",
                            Convert.ToInt32(result) + 1, indexsrc, indextgt);
                        ce.SQLComm.CommandText = sql;
                        ce.SQLComm.ExecuteNonQuery();

                    }
                    else
                    {
                        // Does not exist, we need and update
                        //Console.Write("+");
                        sql = "insert into srctgt (indexsrc,indextgt,freq) values ({0},{1},1)";
                        sql = string.Format(sql, indexsrc, indextgt);
                        ce.SQLComm.CommandText = sql;
                        ce.SQLComm.ExecuteNonQuery();
                    }

                }
            }

            return true;

        }


        static bool AddKey2DB(ConEgg ce, string keyword, string table)
        {
            object result = "";

            string sql = string.Format("select indext from {0} where keyword='{1}'",
                table, keyword);
            ce.SQLComm.CommandText = sql;
            result = ce.SQLComm.ExecuteScalar();
            if (ce.SQLComm.ExecuteScalar() != null)
            {
                // ya existe solo tengo que actualizar
                int keywordfreq;
                sql = string.Format("select keywordfreq from {0} where indext={1}", table, result);
                // somethin like select keywordfreq from SRC where indexSRC=1234", table, result);
                ce.SQLComm.CommandText = sql;
                keywordfreq = Convert.ToInt32(ce.SQLComm.ExecuteScalar()) + 1;

                //Console.Write(" ");
                sql = string.Format("update {0} set keywordfreq={1} where indext={2}",
                    table, keywordfreq, result);
                ce.SQLComm.CommandText = sql;
                ce.SQLComm.ExecuteNonQuery();

            }
            else
            {
                // Does not exist, we need and update
                //Console.Write("-");
                sql = "insert into {0} (keyword,keywordfreq) values ('{1}',1)";
                sql = string.Format(sql, table, keyword);
                ce.SQLComm.CommandText = sql;
                ce.SQLComm.ExecuteNonQuery();
            }


            return true;
        }
    }
    class MyDataBase
    {

        public string DBname { get; set; }
        int devuelve(int variable)
        {
            variable = variable * 3;
            return variable;
        }
        public MyDataBase(string name) { DBname = name; }
        public void DBCreation()
        {
            // Creates de DB
            SQLiteConnection.CreateFile(DBname);
            SQLiteConnection m_dbConnection = new SQLiteConnection("Data Source=MyDatabase.sqlite;Version=3;");
            m_dbConnection.Open();

            string sql = "create table highscores (name varchar(20), score int)";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            command.ExecuteNonQuery();

            sql = "insert into highscores (name, score) values ('Me', 9001)";
            command = new SQLiteCommand(sql, m_dbConnection);
            command.ExecuteNonQuery();

            m_dbConnection.Close();

        }

    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Globalization;

namespace fish03
{
    class Program
    {
        static void Main(string[] args)
        {
            // 1 parameter db
            String[] arguments = Environment.GetCommandLineArgs();
            string[] kk = { "a", "b", "c", "d", "e" };
            string DBname;
            DBname = arguments[1];
            // Console.WriteLine("1 - Cleanning src words with less than 5 instances " + DBname);
            // CleanPopulationV2(DBname,"src");
            // Console.WriteLine("1 - Cleanning tgt words with less than 5 instances " + DBname);
            // CleanPopulationV2(DBname, "tgt");
            // 80% -> z 1.28
            Constants.Z = 1.28;
            Constants.Z = Double.Parse(arguments[2], System.Globalization.CultureInfo.InvariantCulture);
            Console.WriteLine("2 - Setting freqencies for all words in population database " + DBname);
            SetFreqPopulation(DBname, "src");
            SetFreqPopulation(DBname, "tgt");
            // now find de translation
            CreateTargets(DBname, "src");

            CreateTargets(DBname, "tgt");


        }

        static bool CleanPopulationV2(string DBname, string srcOtgt)
        {
            // we will set the frequency and its error for all the population
            //
            // Open connection
            SQLiteConnection m_dbConnection = new SQLiteConnection(string.Format(
                "Data Source={0};Version=3;", DBname));
            m_dbConnection.Open();
            //  begin transaction
            SQLiteCommand command = new SQLiteCommand("begin", m_dbConnection);
            command.ExecuteNonQuery();
            string sql;
            // delete all srctg with less than 5 words
            sql = string.Format(" delete  from srctgt " +
                " where srctgt.index{0} IN " +
                " ( select indext from {0} where keywordfreq < 5 ) ", srcOtgt);
            command.CommandText = sql;
            command.ExecuteNonQuery();
            sql = string.Format(" delete from {0} " +
                " where {0}.keywordfreq < 5 ", srcOtgt);
            command.CommandText = sql;
            command.ExecuteNonQuery();

            command = new SQLiteCommand("end", m_dbConnection);
            command.ExecuteNonQuery();
            m_dbConnection.Close();

            return true;
        }
        static bool CleanPopulation(string DBname, string srcOtgt)
        {
            // we will set the frequency and its error for all the population
            //
            // Open connection
            SQLiteConnection m_dbConnection = new SQLiteConnection(string.Format(
                "Data Source={0};Version=3;", DBname));
            m_dbConnection.Open();
            // datareader command
            SQLiteCommand cmdRDR = new SQLiteCommand(m_dbConnection);

            //  begin transaction
            SQLiteCommand command = new SQLiteCommand("begin", m_dbConnection);
            command.ExecuteNonQuery();
            string sql;
            // let see how many words
            sql = string.Format("SELECT max(indext) from {0}", srcOtgt); // for display %
            cmdRDR.CommandText = sql;
            int maxindext = Convert.ToInt32(cmdRDR.ExecuteScalar());
            // we need to clean all words with less that 5 ocurrences
            // otherwise the error desviation is not correct.
            sql = string.Format("SELECT indext from {0} where keywordfreq < 5", srcOtgt); // At least 5 ocurrences
            cmdRDR.CommandText = sql;
            SQLiteDataReader rdr = cmdRDR.ExecuteReader();
            int indext;
            // int origRow = Console.CursorTop;
            // int origCol = Console.CursorLeft;

            while (rdr.Read())
            {
                indext = Convert.ToInt32(rdr["indext"]);
                // deleting word relation
                sql = string.Format("delete from srctgt where index{0}={1}", srcOtgt, indext);
                command.CommandText = sql;
                command.ExecuteNonQuery();
                // deleting word itself 
                sql = string.Format("delete from {0} where indext={1}", srcOtgt, indext);
                command.CommandText = sql;
                command.ExecuteNonQuery();
                // Console.SetCursorPosition(origCol, origRow);
                // Console.Write(100*indext/maxindext);

            }
            rdr.Close();

            command = new SQLiteCommand("end", m_dbConnection);
            command.ExecuteNonQuery();
            m_dbConnection.Close();
            Console.WriteLine();
            return true;
        }
        static bool SetFreqPopulation(string DBname, string srcOtgt)
        {
            // we will set the frequency and its error for all the population
            //
            // Open connection
            SQLiteConnection m_dbConnection = new SQLiteConnection(string.Format(
                "Data Source={0};Version=3;", DBname));
            m_dbConnection.Open();
            // datareader command
            SQLiteCommand cmdRDR = new SQLiteCommand(m_dbConnection);

            //  begin transaction
            SQLiteCommand command = new SQLiteCommand("begin", m_dbConnection);
            command.ExecuteNonQuery();
            // first we need to know how many words in the population
            string sql;
            sql = string.Format("SELECT sum (keywordfreq) from {0} ", srcOtgt); // sum of all words
            cmdRDR.CommandText = sql;
            double TotalSourcePopulation = Convert.ToDouble(cmdRDR.ExecuteScalar());
            Console.WriteLine(string.Format("Total {0} words population: {1}", srcOtgt, TotalSourcePopulation.ToString()));
            // 
            sql = string.Format("SELECT max(indext) from {0}", srcOtgt);
            cmdRDR.CommandText = sql;
            int maxindext = Convert.ToInt32(cmdRDR.ExecuteScalar());

            // now we need to populate
            sql = string.Format("SELECT indext, keywordfreq from {0} where keywordfreq > 4  ", srcOtgt); // read all words
            cmdRDR.CommandText = sql;
            SQLiteDataReader rdr = cmdRDR.ExecuteReader();
            int indext, keywordfreq;
            double freqpercentage, freqmargin, standrerror, z;
            // int origRow = Console.CursorTop;
            // int origCol = Console.CursorLeft;

            while (rdr.Read())
            {
                indext = Convert.ToInt32(rdr["indext"]);
                keywordfreq = Convert.ToInt32(rdr["keywordfreq"]);
                freqpercentage = Convert.ToDouble(keywordfreq) / TotalSourcePopulation;
                // from http://sphweb.bumc.bu.edu/otlt/MPH-Modules/QuantCore/PH717_ConfidenceIntervals-OneSample/PH717_ConfidenceIntervals-OneSample5.html
                // 80% -> z 1.28
                // 90% -> z 1.645
                // 95% -> z 1.96
                // 98% -> z 2.33
                // 99% -> z 2.56
                z = Constants.Z;
                standrerror = Math.Sqrt(freqpercentage * (1 - freqpercentage) / TotalSourcePopulation);
                // it as freqpercentage is really low -> very close to sqrt(keywordfreq )/TotalSourcePopulation
                freqmargin = standrerror * z;
                // inserting results
                sql = string.Format(CultureInfo.InvariantCulture, "update {0} set  freqpercentage={1}, freqmargin={2} where indext={3}", srcOtgt, freqpercentage, freqmargin, indext);
                command.CommandText = sql;
                command.ExecuteNonQuery();
                // Console.SetCursorPosition(origCol, origRow);
                // Console.Write(100 * indext / maxindext);


            }
            rdr.Close();

            command = new SQLiteCommand("end", m_dbConnection);
            command.ExecuteNonQuery();
            m_dbConnection.Close();
            Console.WriteLine();
            return true;
        }
        static bool CreateTargets(string DBname, string srcOtgt)
        {
            string ContrasrcOtg = "tgt";
            // string cand = "srccand";
            if (srcOtgt == "tgt")
            {
                ContrasrcOtg = "src";
                //cand = "tgtcand";
            }
            // we will set the frequency and its error for all the population
            //
            // Open connection
            SQLiteConnection m_dbConnection = new SQLiteConnection(string.Format(
                "Data Source={0};Version=3;", DBname));
            m_dbConnection.Open();
            // datareader command
            SQLiteCommand cmdALLFILE = new SQLiteCommand(m_dbConnection);
            // command inside each record
            SQLiteCommand cmdNT = new SQLiteCommand(m_dbConnection);
            // begin transaction
            SQLiteCommand cmdT = new SQLiteCommand("begin", m_dbConnection);
            cmdT.ExecuteNonQuery();
            int totalwords, totalwordswithcandidates, numbercandidates; // for screen only
            totalwords = 0; totalwordswithcandidates = 0; numbercandidates = 0; // for screen only
            string sql;
            sql = string.Format("delete from {0}cand", srcOtgt);
            cmdNT.CommandText = sql;
            cmdNT.ExecuteNonQuery();
            sql = string.Format("select {0}.indext from {0} where keywordfreq > 4", srcOtgt); // read all index
            cmdALLFILE.CommandText = sql;
            SQLiteDataReader rdrAllTable;
            rdrAllTable = cmdALLFILE.ExecuteReader();
            while (rdrAllTable.Read())
            {
                int indext = Convert.ToInt32(rdrAllTable["indext"]);
                // let see how many words
                sql = string.Format("select {0}.keywordfreq from {0} where indext={1}", srcOtgt, indext); // number of words
                cmdNT.CommandText = sql;
                int nwords = Convert.ToInt32(cmdNT.ExecuteScalar());
                sql = string.Format("select {0}.keyword from {0} where indext={1}", srcOtgt, indext); // number of words
                cmdNT.CommandText = sql;
                string SourceWord = Convert.ToString(cmdNT.ExecuteScalar());
                //                if (SourceWord== "les")
                //                {
                //                    int i = 0;
                //                    i=i*i;
                //                }
                // let      see how many words
                sql = string.Format("select sum (srctgt.freq) from {0} " +
                    "join srctgt on {0}.indext=srctgt.index{0} " +
                    "join {1} on {1}.indext = srctgt.index{1}" +
                    " where {0}.indext = {2}", srcOtgt, ContrasrcOtg, indext); //  all words for source word
                cmdNT.CommandText = sql;
                int nsamplewords = Convert.ToInt32(cmdNT.ExecuteScalar());
                // otherwise the error desviation is not correct.
                sql = string.Format("select srctgt.freq, {1}.keyword," +
                    " {1}.freqpercentage, {1}.freqmargin, srctgt.index{1} from {0} " +
                    "join srctgt on {0}.indext=srctgt.index{0} " +
                    "join {1} on {1}.indext = srctgt.index{1} " +
                    "where {0}.indext = {2} and srctgt.freq > 5 ", srcOtgt, ContrasrcOtg, indext); //
                cmdNT.CommandText = sql;
                SQLiteDataReader rdr = cmdNT.ExecuteReader();
                double freqSample, freqpercentageSample, freqmarginSample, freqpercentage, freqmargin;
                double populationsampleratio, freqinsample;
                double z, standrerror;
                string WordTargetSample;
                string insertSQL = "insert into {6}cand (indexsrc, indextgt, freqpercentagesample, " +
                    "freqmarginsample, populationsampleratio, freqinsample) " +
                    "values ({0},{1},{2},{3},{4},{5} ) ";
                bool fuera = false;
                int ncandidates = 0; // number of candidates (for screeninfo)
                int maxratio = 0; // maxium dif (for screeninfo)
                string maxcandidate = ""; // word with maxratio (for screeninfo)
                int maxfreq = 0; // max freq (for screeninfo )
                totalwords += 1;
                while (rdr.Read())
                {
                    WordTargetSample = Convert.ToString(rdr["keyword"]);
                    // if (WordTargetSample== "los")
                    //  {
                    //     int i = 3;
                    //  }
                    freqpercentage = Convert.ToDouble(rdr["freqpercentage"]); // from the population
                    freqmargin = Convert.ToDouble(rdr["freqmargin"]); // from the population
                    freqSample = Convert.ToDouble(rdr["freq"]);
                    freqpercentageSample = (double)freqSample / (double)nsamplewords;
                    freqinsample = (double)freqSample / (double)nwords;
                    z = Constants.Z;
                    standrerror = Math.Sqrt(freqpercentageSample * (1 - freqpercentageSample) / nsamplewords);
                    freqmarginSample = standrerror * z;
                    populationsampleratio = (freqpercentageSample / freqpercentage);
                    // match if Sample outside Populations
                    fuera = false; // fuera de lo previsto
                    if ((freqpercentage + freqmargin) < (freqpercentageSample - freqmarginSample)) { fuera = true; }
                    // if ((freqpercentage - freqmargin) > (freqpercentageSample + freqmarginSample)) { fuera = true; }
                    if (fuera == true) // freqsammple is significative but
                    {
                        // we ask at least 10% frequencency
                        if (freqinsample <= 0.1)
                        {
                            fuera = false;
                            // int tmpv = Convert.ToInt32   (porcentaje * 100);
                            // Console.WriteLine(SourceWord + "-" + WordTargetSample + "-"+ Convert.ToString(tmpv));
                            // Console.ReadLine();

                        } // if freq is less 10 % ignore
                    }
                    if (fuera)
                    {
                        //Console.WriteLine("{0} ({1}) FreqSam/Pob {2}/{3} -> {4}/{5}", WordTargetSample, freqSample,
                        // freqpercentageSample, freqpercentage, Convert.ToInt32(populationsampleratio) );
                        if (Convert.ToInt32(populationsampleratio) > maxratio)
                        {
                            maxratio = Convert.ToInt32(populationsampleratio);
                            maxfreq = Convert.ToInt32(freqinsample * 100);
                            maxcandidate = WordTargetSample;
                        }
                        ncandidates += 1;
                        sql = string.Format(CultureInfo.InvariantCulture, insertSQL, indext, rdr["index" + ContrasrcOtg],
                            freqpercentageSample, freqmarginSample, populationsampleratio, freqinsample, srcOtgt);
                        cmdT.CommandText = sql;
                        cmdT.ExecuteNonQuery();
                        numbercandidates += 1; //sreen only
                    }
                }
                rdr.Close();
                if (ncandidates > 0)
                {
                    totalwordswithcandidates += 1; //screen only
                    Console.WriteLine("WRD {0} / Inst {1} -> cand {2} maxR {3} -> {4}/{5} ",
                        SourceWord, nwords, ncandidates, maxratio, maxcandidate, maxfreq);
                }
                else
                {
                    Console.WriteLine("WRD {0} / Inst {1} ", SourceWord, nwords);
                }
                maxcandidate = "";





            }
            rdrAllTable.Close();
            cmdT = new SQLiteCommand("end", m_dbConnection);
            cmdT.ExecuteNonQuery();


            m_dbConnection.Close();
            Console.WriteLine();
            Console.WriteLine("Total words {0} with candidates {1} ({2}%) Total candidates {3} with {4}",
                totalwords, totalwordswithcandidates,
                Convert.ToInt32(100.0 / totalwords * totalwordswithcandidates),
                numbercandidates, Constants.Z);
            return true;
        }

    }
    static class Constants
    {
        // https://stackoverflow.com/questions/14368129/c-sharp-global-variables
        //
        // from http://sphweb.bumc.bu.edu/otlt/MPH-Modules/QuantCore/PH717_ConfidenceIntervals-OneSample/PH717_ConfidenceIntervals-OneSample5.html
        // 80% -> z 1.28
        // 90% -> z 1.645
        // 95% -> z 1.96
        // 98% -> z 2.33
        // 99% -> z 2.56
        public static double Z = 2.56;


    }
}

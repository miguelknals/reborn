using System.Data.SQLite;
using System;
using System.Data;
using System.IO;
using System.Text;

using fishCoCommon;

namespace fish04Co
{
    class Program
    {

        static void Main(string[] args)
        {
            // 1 parameter db
            // 2 parameter src file
            // 3 parameter tgt file
            String[] arguments = Environment.GetCommandLineArgs();
            string DBname;
            string srcfile;
            string tgtfile;
            double liminf;
            double limsup;
            //
            DBname = arguments[1];
            srcfile = arguments[2];
            tgtfile = arguments[3];
            liminf = Double.Parse(arguments[4], System.Globalization.CultureInfo.InvariantCulture);
            limsup = Double.Parse(arguments[5], System.Globalization.CultureInfo.InvariantCulture);
            // liminf = 0.3; limsup = 100;
            // srcfile = (@"U:\tmp\fisher\DOGC_VERIFY_BK.ca");
            // tgtfile = (@"U:\tmp\fisher\DOGC_VERIFY_BK.es");


            StreamReader srsrc = new StreamReader(srcfile, Encoding.UTF8, true); // input file
            StreamReader srtgt = new StreamReader(tgtfile, Encoding.UTF8, true); // input file



            SentenceSpliterClass SScandsrc;
            SentenceSpliterClass SScandtgt;
            // now I have to find all posible targets for each word
            // DBname = @"U:\tmp\fisherD\laikaV2.db";
            // Open connection
            SQLiteConnection m_dbConnection = new SQLiteConnection(string.Format(
                "Data Source={0};Version=3;", DBname));
            var command = new SQLiteCommand(m_dbConnection);
            var da = new SQLiteDataAdapter();
            string sqlaux, sql, sqlaux2, sqlunrestricted;
            string sqlauxI, sqlaux2I; // inverse SQL
            sqlaux = "select src.keyword, tgt.keyword, srccand.freqinsample from src " +
                "join srccand on src.indext=srccand.indexsrc " +
                "join tgt on srccand.indextgt=tgt.indext " +
                "where src.keyword in ( {0} ) " +
                "and tgt.keyword in ( {1} ) " +
                "order by freqinsample DESC ";
            sqlauxI = "select tgt.keyword, src.keyword, tgtcand.freqinsample from tgt " + // Same
                "join tgtcand on tgt.indext=tgtcand.indexsrc " + //note at the end src is OK
                "join src on tgtcand.indextgt=src.indext " + // the indextgt is ok
                "where tgt.keyword in ( {0} ) " +
                "and src.keyword in ( {1} ) " +
                "order by freqinsample DESC ";
            sqlaux2 = "select src.keyword, tgt.keyword, srccand.freqinsample from src " +
                "join srccand on src.indext=srccand.indexsrc " +
                "join tgt on srccand.indextgt=tgt.indext " +
                "where src.keyword in ( {0} ) " +
                "order by freqinsample DESC ";
            sqlaux2I = "select tgt.keyword, src.keyword, tgtcand.freqinsample from tgt " + // Same
                "join tgtcand on tgt.indext=tgtcand.indexsrc " + //note at the end src is OK
                "join src on tgtcand.indextgt=src.indext " + // the indextgt is ok
                "where tgt.keyword in ( {0} ) " +
                "order by freqinsample DESC ";

            // test 
            // Bulk loop reading strings
            int nlines = 0;
            int nlinesbad = 0;
            // for totals
            int[] punct = new int[101];
            int[] totoov = new int[101];
            int[] totunktra = new int[101];
            int[] totnword = new int[101];
            int ninfo, ninfocurrent, ninfototal;
            ninfo = 500; ninfocurrent = ninfo; ninfototal = 0;

            var logOk = new logger(@"C:\VS2017\fisher\logOK.txt");
            var logOOV = new logger(@"C:\VS2017\fisher\logOOV.txt");
            var logBadSrc = new logger(@"C:\VS2017\fisher\logBadSrc.txt");
            var logBadTgt = new logger(@"C:\VS2017\fisher\logBadTgt.txt");
            var logBadAll = new logger(@"C:\VS2017\fisher\logBadAll.txt");
            logOk.LogString("Hi babe");
            logOk.Close(); logOOV.Close(); logBadAll.Close(); logBadSrc.Close(); logBadTgt.Close();



            try
            {
                // CE = OpenDB(DBname); //CE has connection an command.
                m_dbConnection.Open();

                using (srsrc)
                {
                    using (srtgt)
                    {
                        while (srsrc.Peek() != -1)
                        {

                            // Read the streams
                            String linesrc = srsrc.ReadLine();
                            String linetgt = srtgt.ReadLine();
                            nlines += 1;
                            SScandsrc = new SentenceSpliterClass(linesrc, false);
                            SScandtgt = new SentenceSpliterClass(linetgt, false);
                            SScandsrc.Split(); SScandtgt.Split();
                            // create structure to target score
                            var sp = new SentencePairScoreClass(SScandsrc, SScandtgt);
                            sp.debuga = true;
                            // now search results in db stored in dataset
                            sql = string.Format(sqlaux, sp.auxsrc, sp.auxtgt);
                            sqlunrestricted = string.Format(sqlaux2, sp.auxsrc);
                            command.CommandText = sql;
                            DataSet ds = new DataSet();
                            DataSet dsunrestricted = new DataSet();
                            da = new SQLiteDataAdapter(command);
                            da.Fill(ds, "WORDSCORE");
                            command.CommandText = sqlunrestricted;
                            da = new SQLiteDataAdapter(command);
                            da.Fill(dsunrestricted, "WORDSCORE");
                            sp.Scoresrc(ds, dsunrestricted);
                            // Inverse sentence
                            var spI = new SentencePairScoreClass(SScandtgt, SScandsrc);
                            spI.debuga = true;
                            // now search results in db stored in dataset
                            sql = string.Format(sqlauxI, sp.auxtgt, sp.auxsrc);
                            sqlunrestricted = string.Format(sqlaux2I, sp.auxtgt);
                            command.CommandText = sql;
                            DataSet dsI = new DataSet();
                            DataSet dsIunrestricted = new DataSet();
                            da = new SQLiteDataAdapter(command);
                            da.Fill(dsI, "WORDSCORE");
                            command.CommandText = sqlunrestricted;
                            da = new SQLiteDataAdapter(command);
                            da.Fill(dsIunrestricted, "WORDSCORE");
                            spI.Scoresrc(dsI, dsIunrestricted);
                            //  scores are in
                            // sp.closetoperfection and spI.closetoperfection
                            string auxs;
                            Console.WriteLine(linesrc);
                            Console.WriteLine(linetgt);
                            auxs = "                    | ".Remove(sp.closetoperfection / 5, 1).Insert(sp.closetoperfection / 5, ">");
                            Console.WriteLine(auxs);
                            auxs = "                    |".Remove(spI.closetoperfection / 5, 1).Insert(spI.closetoperfection / 5, ">");
                            Console.WriteLine(auxs);
                            // as the score is bases in sum of words, lets say less than 5 we ignored
                            sp.looksfine = true; spI.looksfine = true;
                            if (sp.looksfine)
                            {
                                if (sp.nmatchedu < 5)
                                {
                                    sp.looksfine = false; sp.rejectinfo = "LT5"; // Less than 5 matches";
                                }
                            }
                            if (spI.looksfine)
                            {
                                if (spI.nmatchedu < 5)
                                {
                                    spI.looksfine = false; spI.rejectinfo = "LT5"; // Less than 5 matches";
                                }
                            }
                            if (spI.looksfine | sp.looksfine)
                            {
                                if (sp.closetoperfection < 80)
                                {
                                    // source quality is bad
                                    sp.looksfine = false; sp.rejectinfo = "SRCQualityBab";
                                }
                                if (spI.closetoperfection < 80)
                                {
                                    spI.looksfine = false; spI.rejectinfo = "TGTQualityBab";
                                }
                            }
                            // OOV
                            //if (sp.looksfine & spI.looksfine)
                            // {
                            sp.OOVbalanced = true; spI.OOVbalanced = true;
                            int percOOV = (sp.nsrc - sp.noov) * 100 / sp.nsrc;
                            int percOOVI = (spI.nsrc - spI.noov) * 100 / spI.nsrc;
                            // max 20% dif
                            if (Math.Abs(percOOV - percOOVI) > 20)
                            { // difference too big
                                sp.OOVbalanced = false;
                                spI.OOVbalanced = false;
                                string auxS;
                                auxS = sp.noov.ToString();
                                foreach (WordScoreClass w in sp.WordScoresrc)
                                {
                                    if (w.matched_unrestricted) // is an OOV
                                    {
                                        auxS += " " + w.word;
                                    }
                                }
                                logOOV.LogString(auxS);
                                foreach (WordScoreClass w in spI.WordScoresrc)
                                {
                                    if (w.matched_unrestricted) // is an OOV
                                    {
                                        auxS += " " + w.word;
                                    }
                                }
                                logOOV.LogString(auxS);



                            }
                            // }
                            // I have my decision and the reject info
                            if (sp.looksfine & spI.looksfine)
                            {

                            }
                            else
                            {
                                Console.WriteLine("SRC->" + sp.rejectinfo);
                                Console.WriteLine("TGT->" + spI.rejectinfo);
                                Console.ReadLine();
                            }




                            // 


                            // if (debuga) { Console.WriteLine("Intro to continue "); Console.ReadLine(); } 
                            // liminf = 0.3; limsup = 100;

                            if (sp.nmatchedu >= 500000)
                            {
                                if (sp.closetoperfection <= liminf)
                                {
                                    nlinesbad += 1;
                                    punct[sp.closetoperfection] += 1;
                                    totoov[sp.closetoperfection] += sp.noov;
                                    totunktra[sp.closetoperfection] += sp.nunktra;
                                    totnword[sp.closetoperfection] += sp.nsrc;
                                    Console.WriteLine(sp.sbsentence.ToString());
                                    Console.WriteLine(linesrc);
                                    Console.WriteLine(linetgt);
                                    Console.WriteLine("Intro to continue "); Console.ReadLine();
                                    ninfocurrent -= 1;
                                    if (ninfocurrent == 0)
                                    {
                                        ninfototal += ninfo;
                                        ninfocurrent = ninfo; Console.WriteLine(ninfototal);
                                        bool aborta = false;
                                        if (aborta) { break; }
                                    }

                                }
                            }

                            // Ready for next sentence

                        }
                        Console.WriteLine(string.Format("File {0} -> {1} lines", srcfile, nlines));
                        Console.WriteLine("punct, totoov, totunktra, totnword");
                        for (int i = 0; i <= 101; i++)
                        {
                            Console.WriteLine("{0}, {1}, {2}, {3}", punct[i], totoov[i], totunktra[i], totnword[i]);
                        }
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
            finally
            {
                m_dbConnection.Close();

            }
            // CloseDB(CE);
            Console.WriteLine(string.Format("lines/badlines {0}/{1}", nlines, nlinesbad));

            Console.WriteLine("The End");
            // Console.ReadLine();


        }

        class WordScoreClass
        {
            public string word { get; set; }
            public string counterword { get; set; }
            public string counterword_unrestricted { get; set; }
            public bool matched { get; set; }
            public bool matched_unrestricted { get; set; }
            public double score { get; set; }
            public double score_unrestricted { get; set; }
            public double difscore { get; set; }
            public int pass { get; set; }
            public WordScoreClass()
            { word = ""; matched = false; score = 0; pass = 0; }
        }



        class SentencePairScoreClass
        {
            // this class has the score of the source and the target
            // plus sentence infor             
            public WordScoreClass[] WordScoresrc { set; get; }
            public WordScoreClass[] WordScoretgt { set; get; }
            public string originalsentence { set; get; }
            public int nsrc { set; get; } // number of words
            public int ntgt { set; get; } // number of words
            public StringBuilder sbsentence { get; set; }
            public bool debuga { get; set; }
            public string auxsrc { get; set; } // list of words delimited with quotes for sql
            public string auxtgt { get; set; } // list of words delimited with quotes for sql
            public int nmatched { get; set; }
            public int nmatchedu { get; set; }
            public int noov { get; set; } // number of oov in src (even with unkrestricted)
            public int nunktra { get; set; } // words in src with unknown translation
            public int closetoperfection { get; set; }
            public bool looksfine { get; set; }
            public bool OOVbalanced { get; set; }
            public string rejectinfo { get; set; }
            public SentencePairScoreClass(SentenceSpliterClass SScandsrc, SentenceSpliterClass SScandtgt)
            {  // we use the splited sentences  
                debuga = false; looksfine = false; rejectinfo = "";
                originalsentence = SScandsrc.originalsentence;
                sbsentence = new StringBuilder("");
                nsrc = SScandsrc.words.Length - 1; // number of words
                ntgt = SScandtgt.words.Length - 1; // number of words
                WordScoresrc = new WordScoreClass[nsrc + 1]; // has to be actual number of words 
                WordScoretgt = new WordScoreClass[ntgt + 1]; // has to be actual number of words 
                //string auxsrc = " ";               
                for (int i = 0; i <= nsrc; i += 1)
                {
                    auxsrc += "'" + SScandsrc.words[i] + "',";
                    WordScoresrc[i] = new WordScoreClass();
                    WordScoresrc[i].word = SScandsrc.words[i];
                }
                // string auxtgt = " ";
                for (int i = 0; i <= ntgt; i += 1)
                {
                    auxtgt += "'" + SScandtgt.words[i] + "',";
                    WordScoretgt[i] = new WordScoreClass();
                    WordScoretgt[i].word = SScandtgt.words[i];
                }
                auxsrc = auxsrc.Remove(auxsrc.Length - 1);
                auxtgt = auxtgt.Remove(auxtgt.Length - 1);

            }
            public void Scoresrc(DataSet ds, DataSet dsunrestricted)
            {
                // uses scores from dataset to find out score punctuation

                string wsrc, wtgt;
                Single score;
                // first round RESTRICTED
                if (debuga) { sbsentence.AppendLine("Restricted:"); }
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    // datset gives aus src, tgt, freq. The idea is all items in the dataset
                    // are relations src<>tgt significatives, so we do not care the score but
                    // just the one more usual. We will iterate thru oar WordScocre (src or 
                    // target) if we find a match, we will assign the word and the score
                    // score is % times that src is tgt
                    wsrc = dr[0].ToString(); // source 
                    wtgt = dr[1].ToString(); // target 
                    score = Convert.ToSingle(dr[2]); // my best candidate 
                    bool somethingdone = false;
                    foreach (WordScoreClass e in WordScoresrc)
                    {
                        if (e.matched == false) // this source word still has no match                    
                        {
                            somethingdone = true;
                            if (wsrc == e.word) // this source has a target 
                            {
                                e.counterword = wtgt; e.score = score; e.matched = true;
                                if (debuga) { sbsentence.AppendLine(string.Format("{0} {1} {2}", wsrc, wtgt, score)); }
                            }
                        }
                    }
                    if (somethingdone == false) { break; } // if all words assigned leave
                }
                // second round now UNRESTRICTED. Same as before but SQL is not restricted
                // by target words. Loop changes as we will ony find a score diference that is
                // percentaje unrestricted by target - percentaje restricted by my translation
                if (debuga) { sbsentence.AppendLine("Unrestricted:"); }
                foreach (DataRow dr in dsunrestricted.Tables[0].Rows)
                {
                    wsrc = dr[0].ToString(); // source 
                    wtgt = dr[1].ToString();
                    score = Convert.ToSingle(dr[2]); // my best candidate 
                    bool somethingdone = false;
                    foreach (WordScoreClass e in WordScoresrc)
                    {
                        if (e.matched_unrestricted == false) // this source word still has no match                    
                        {
                            somethingdone = true;
                            if (wsrc == e.word) // this source has a target 
                            {
                                e.counterword_unrestricted = wtgt; e.score_unrestricted = score; e.matched_unrestricted = true;
                                // if (e.matched) { e.difscore = e.score_unrestricted - e.score; } // only if I have an score
                                if (debuga) { sbsentence.AppendLine(string.Format("{0} {1} {2}", wsrc, wtgt, score)); }
                                // break;
                            }
                        }
                    }
                    if (somethingdone == false) { break; }
                }
                // I have for each word its src<>tgt more probable and the src more probalbe
                // without restriction
                // 
                // int nmatched, nmatchedu;

                int lensentence = WordScoresrc.Length;
                double sumdif; sumdif = 0;
                double sumscoresentence, sumscoresentenceu;
                sumscoresentence = 0; sumscoresentenceu = 0;
                // this loop is to count matched src<>tgt
                // src<>tgt unrestricted
                nmatched = 0; nmatchedu = 0; // number matched
                foreach (WordScoreClass e in WordScoresrc)
                {
                    if (e.matched) { nmatched += 1; }
                    if (e.matched_unrestricted) { nmatchedu += 1; }
                    if (e.matched_unrestricted) // (e.matched && e.matched_unrestricted)
                    {  // only we have a pair restricted / unrestricted
                        e.difscore = e.score_unrestricted - e.score;
                        sumdif += e.difscore; // we calculate sum of all dif for std dev.
                        // sumscore sentence max 1 per word
                        sumscoresentence += e.score; sumscoresentenceu += e.score_unrestricted; //sum of score                        
                    }

                }
                // this code is for pretty printing
                // this code is for pretty printing
                int sposi, sposiu; sposi = 0; sposiu = 0; // string positions
                string auxs = "";
                auxs = "          ";
                string type; type = "";
                if (debuga)
                {
                    foreach (WordScoreClass e in WordScoresrc)
                    {
                        auxs = "           "; type = " ";
                        if (e.matched)
                        {
                            sposi = Convert.ToInt32(e.score * 10);
                            auxs = auxs.Remove(sposi, 1).Insert(sposi, ">");
                        }
                        if (e.matched_unrestricted)
                        {
                            sposiu = Convert.ToInt32(e.score_unrestricted * 10);
                            auxs = auxs.Remove(sposiu, 1).Insert(sposiu, "|");
                        }
                        if (e.matched && e.matched_unrestricted)
                        {
                            if (sposi == sposiu) { auxs = auxs.Remove(sposi, 1).Insert(sposi, "*"); }
                        }
                        if (e.matched == false) { type = "+"; } // for display
                        if (e.matched_unrestricted == false) { type = "*"; } // for display
                        if (e.counterword == e.counterword_unrestricted) // for display
                        {
                            sbsentence.AppendLine(string.Format("{0} {1} {2} - {3} -{4} ",
                            type, auxs, e.word, e.counterword, e.difscore));
                        }
                        else
                        {
                            sbsentence.AppendLine(string.Format("{0} {1} {2} - {3} ({4}) - {5}",
                            type, auxs, e.word, e.counterword, e.counterword_unrestricted, e.difscore));
                        }
                    }
                }
                // end of code pretty printing this code is for pretty printing
                //
                // we will calculate meandif and standar dev dif, but we will not use.
                // this can lead to another way of evaluate as a perfect translation
                // should have no difference or minimal differenes
                double meandif; // mean value of diference between unrestricted score and score
                double stddevdif; // std value of diference between unrestricted score and score
                // int noov;
                // int nunktra; // unknown translation
                stddevdif = 0;
                noov = 0; nunktra = 0;
                meandif = sumdif / nmatched;
                // this loops calculate nunktra, noov (and used for standard deviation)
                foreach (WordScoreClass e in WordScoresrc)
                {
                    if (e.matched) // we have restriucted translation
                    {
                        stddevdif += (e.difscore - meandif) * (e.difscore - meandif);
                    }
                    else
                    {
                        if (e.matched_unrestricted) { nunktra += 1; } else { noov += 1; }

                    }
                }
                stddevdif = stddevdif / (nmatched - 1); stddevdif = Math.Sqrt(stddevdif);
                // int closetoperfection = 0; // 0 if sumscoressentenceu =0 
                // this is the real punctuation 0 and 100
                if (sumscoresentenceu > 0) { closetoperfection = Convert.ToInt32(100 * sumscoresentence / sumscoresentenceu); }

                auxs = "WRD {0}/ M {1}/ U {2}/ OOV {3} - Sco {4:N2}-{5:N2} Mean/STD {6:N2}-{7:N2} - C2P {8}  ";
                sbsentence.AppendLine(string.Format(auxs, nsrc + 1, nmatchedu,
                    nunktra, noov, sumscoresentenceu, sumscoresentence, meandif, stddevdif, closetoperfection));
                if (debuga) { Console.WriteLine(sbsentence.ToString()); }

            }
        }

        public class logger
        {
            private StreamWriter sw;
            public logger(string fileName)
            {
                sw = new StreamWriter(fileName, true);
            }

            public void LogString(string txt)
            {
                sw.WriteLine(txt);
                sw.Flush();
            }

            public void Close()
            {
                sw.Close();
            }
        }
    }
}

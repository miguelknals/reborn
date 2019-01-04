using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace fishCoCommon
{
    class CommonClass
    {
        public string originalsentence { get; set; }
        public void SegmentClass(string inputsentence) { originalsentence = inputsentence; }
    }
    public class SentenceSpliterClass
    {
        public string originalsentence { get; set; }
        public string[] words { get; set; }
        public bool TodoOk {get; set;}
        public bool removedupwords { get; set; }
        public string info { get; set; }
        public int lensentence { get; set; }
        public SentenceSpliterClass(string inputsentence, bool removeduplicatedwords)
           { originalsentence = inputsentence; removedupwords = removeduplicatedwords; }
        public void  Split ()
        {
            // apostrophe quotes
            //
            // \u2018  LEFT SINGLE QUOTATION MARK	‘
            // \u2019 ’ 
            // \u201C LEFT DOUBLE QUOTATION MARK	“ 	
            // \u201D RIGHT DOUBLE QUOTATION MARK	”
            char[] tipochar = new char[] {
                '.', ';', '¿', '?', '!','¡',':', '\'', ',', '-',
                '0', '1','2','3','4','5','6','7','8','9',
                '\u2018', '\u2019',  '\u201C', '\u201D', 
                ')', '(', '=','+','*','/'

            };
            TodoOk = true; info = ""; // Optimism
            string line = originalsentence;
            line = line.Trim();
            lensentence = originalsentence.Length;
            if (lensentence==0) { TodoOk = false; info = "Sentence with 0 lengh"; return; }
            // int index = line.IndexOfAny(tipochar); // quote replacing
            // if (index != -1) // cleanup
            //{
                foreach (char c in tipochar) 
                {
                    line = line.Replace(c.ToString(), " ");
                }
            //}
            //int index = line.IndexOfAny(tipochar); // quote replacing
            //while (index != -1)
            //{
            //    line = line.Replace(line[index].ToString(), " ");
            //    index = line.IndexOfAny(tipochar); // quote replacing
           // }


            line = line.ToLower();
            if (removedupwords)
            {
                words = line
                    .Split()
                    .Where(x => x != string.Empty)
                    .Distinct().ToArray();
                // wordssrc = linesrc.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            }
            else
            {
                words = line
                .Split()
                .Where(x => x != string.Empty)
                .ToArray();
                // wordssrc = linesrc.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            }

            TodoOk = true;
            

            return;

        }
    }
}

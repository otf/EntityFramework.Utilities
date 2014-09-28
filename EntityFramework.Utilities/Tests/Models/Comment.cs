using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tests.FakeDomain.Models
{
    public class Comment
    {
        public int Id { get; set; }
        public string Text { get; set; }
        public int PostId { get; set; }
        public BlogPost Post { get; set; }
        public ICollection<SubComment> SubComments { get; set; }
    }

    public class SubComment
    {
        public int Id { get; set; }
        public string Text { get; set; }
        public int CommentId { get; set; }
        public Comment Comment { get; set; }
    }
}

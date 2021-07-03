package demo;

public class ReverseList {
    public static void main(String[] args) {
        TreeNode head = new TreeNode(1);
        head.next = new TreeNode(2);
        head.next.next = new TreeNode(3);
        head.next.next.next = new TreeNode(4);
        head.next.next.next.next = new TreeNode(5);

        TreeNode h = reverse(head, 3);
        System.out.println(h.val);
        System.out.println(h.next.val);
        System.out.println(h.next.next.val);
        System.out.println(h.next.next.next.val);
        System.out.println(h.next.next.next.next.val);
    }

    public static TreeNode reverse(TreeNode head, int k) {
        if (head == null) return null;

        TreeNode dummy = new TreeNode(-1);
        dummy.next = head;
        TreeNode pre = null;
        TreeNode current = head;
        TreeNode tmp = null;

        for (int i = 1; i <= k; i++) {
            tmp = current.next;
            current.next = pre;
            pre = current;
            current = tmp;
        }

        dummy.next.next = tmp;

        return pre;
    }

}

class TreeNode {
    int val;
    TreeNode next;

    public TreeNode(int val) {
        this.val = val;
    }
}
